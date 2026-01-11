using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine
{
    public class CellManager(SessionState sessionState, IFileSystem fileSystem, IFolderManager folderManager, IIndexManager indexManager) : ICellManager
    {
        private readonly Dictionary<string,CellEnvironment> _cachedEnvironment = new();

        public async Task<Result> CreateCellEnvironment(string cellName, Column[] columns)
        {
            CellEnvironment env = new CellEnvironment
            {
                Columns = columns.ToArray()
            };
            return await SaveEnvironment(env, cellName);
        }

        public async Task<Result> UpdateCellEnvironment(string cellName, Column[] columns)
        {
            var envResult = await GetEnvironment(cellName);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;

            env.Columns = columns;

            return await SaveEnvironment(env, cellName);
        }

        public Task<Result> CreateCellInstance(string cellGroupName, string instanceHash, Row instanceData, Column[] schema)
        {
            // 1. Save main data
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);

            var rowBytesResult = RowSerializer.Serialize(instanceData);
            if (!rowBytesResult.TryGetValue(out var rowBytes)) return Task.FromResult<Result>(rowBytesResult.Error);

            var existsResult = indexManager.CellDataExists(cellGroupName, hashBytes);
            if (existsResult.IsFailure) return Task.FromResult<Result>(existsResult.Error);
            if (existsResult.Value) return Task.FromResult<Result>(new Error(ErrorPrefixes.DataError, "Cell instance already exists"));
            
            var insertDataResult = indexManager.InsertCellData(cellGroupName, hashBytes, rowBytes);
            if (insertDataResult.IsFailure) return Task.FromResult<Result>(insertDataResult.Error);

            // 2. Update property indexes
            foreach (var col in schema.Where(c => c.IsIndex || c.IsPrimaryKey))
            {
                var value = instanceData[col.Name];
                if (value != null)
                {
                    var keyBytes = IndexKeyConverter.ToByteArray(value);
                    var insertIndexResult = indexManager.InsertCellPropertyIndex(cellGroupName, col.Name, keyBytes, hashBytes);
                    if (insertIndexResult.IsFailure) return Task.FromResult<Result>(insertIndexResult.Error);
                }
            }
            return Task.FromResult(Result.Success());
        }

        public async Task<Result<Row>> GetCellInstance(string cellGroupName, string instanceHash)
        {
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);

            var rowBytesResult = indexManager.GetCellData(cellGroupName, hashBytes);
            if (rowBytesResult.IsSuccess)
            {
                var envResult = await GetEnvironment(cellGroupName);
                if (!envResult.TryGetValue(out var env)) return envResult.Error;

                var rowResult = RowSerializer.Deserialize(rowBytesResult.Value, env.Columns);
                return rowResult;
            }

            return rowBytesResult.Error;
        }

        public async Task<Result<IEnumerable<Row>>> GetAllCellInstances(string cellGroupName)
        {
            var allDataResult = indexManager.GetAllCellData(cellGroupName);
            if (allDataResult.IsFailure) return allDataResult.Error;

            var envResult = await GetEnvironment(cellGroupName);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;

            var rows = new List<Row>();
            foreach (var kvp in allDataResult.Value)
            {
                var rowResult = RowSerializer.Deserialize(kvp.Value, env.Columns);
                if (rowResult.IsSuccess) rows.Add(rowResult.Value);
            }

            return rows;
        }

        public async Task<Result> CreateTable(string cellName, Table table)
        {
            var env = await GetEnvironment(cellName);
            if (!env.TryGetValue(out var cellEnviroument)) return env.Error;

            var cellTables = cellEnviroument.Tables.ToList();
            cellTables.Add(table);
            cellEnviroument.Tables = cellTables.ToArray();
            var saveResult = await SaveEnvironment(cellEnviroument, cellName);
            if(saveResult.IsFailure) return saveResult.Error;
            
            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();

            var tablePath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, table.Name);
            try
            {
                if(!fileSystem.Directory.Exists(tablePath)) fileSystem.Directory.CreateDirectory(tablePath);
            }
            catch (Exception ex)
            {
                return new Error(ErrorPrefixes.FileError, $"Failed to create directory for table '{table.Name}': {ex.Message}");
            }
            return Result.Success();
        }

        public async Task<Result> DeleteTable(string cellName, string tableName)
        {
            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();

            var envResult = await GetEnvironment(cellName);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;

            var tableToDelete = env.Tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
            if (tableToDelete == null) return new Error(ErrorPrefixes.DataError, $"Table '{tableName}' not found in cell '{cellName}'.");

            string tablePath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, tableName);
            string trashPath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, "trash_" + tableName);

            try { folderManager.RenameFolder(tablePath, trashPath); }
            catch { return new Error(ErrorPrefixes.FileError, "Could not prepare table for deletion (Folder Rename Failed)."); }

            env.Tables = env.Tables.Where(t => t != tableToDelete).ToArray();

            try
            {
                var result = await SaveEnvironment(env, cellName);
                if (result.IsFailure) return result.Error;
            }
            catch
            {
                try
                {
                    folderManager.RenameFolder(trashPath, tablePath);
                    envResult.Value.Tables = envResult.Value.Tables.Append(tableToDelete).ToArray();
                }
                catch{ return new CriticalError(); }
                
                return new Error(ErrorPrefixes.FileError, "Failed to update cell definition. Deletion reverted.");
            }

            try { folderManager.DeleteFolder(trashPath); }
            catch { return Result.Success(); }

            return Result.Success();
        }

        public async Task<Result<CellEnvironment>> GetEnvironment(string cellName)
        {
            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();

            if (_cachedEnvironment.TryGetValue(cellName, out var env)) return env;

            var filePathResult = GetCellFilePath(cellName, "_env");
            if(filePathResult.IsFailure) return filePathResult.Error;
            if (!fileSystem.File.Exists(filePathResult.Value)) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(fileSystem, filePathResult.Value, CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");
            
            env = ByteObjectConverter.ByteArrayToObject<CellEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            _cachedEnvironment[cellName] = env;
            return env;
        }

        private Result<string> GetCellFilePath(string cellName, string fileName)
        {
            if(sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
            return fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, fileName);
        }
        
        private async Task<Result> SaveEnvironment(CellEnvironment env, string cellName)
        {
            var filePath = GetCellFilePath(cellName, "_env");
            if(filePath.IsFailure) return filePath.Error;

            var bytes = ByteObjectConverter.ObjectToByteArray(env);

            await ByteReaderWriter.WriteBytesToFile(fileSystem, filePath.Value, bytes, CancellationToken.None);
            _cachedEnvironment[cellName] = env;
            return Result.Success();
        }
        
        public async Task<Result<Column[]>> GetColumnsFromTable(string cellName, string tableName)
        {
            var env = await GetEnvironment(cellName);
            if(env.IsFailure) return env.Error;

            var table = env.Value.Tables.FirstOrDefault(t => t.Name == tableName);
            if(table is null) return new Error(ErrorPrefixes.StateError, "Table does not exist in cell environment");

            return table.Columns.ToArray();
        }

        public async Task<Result> DropColumnAsync(string cellName, string columnName)
        {
            // 1. Get the current environment (the "before" schema)
            var envResult = await GetEnvironment(cellName);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;

            var oldColumns = env.Columns;
            var columnToRemove = oldColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (columnToRemove == null)
                return new Error(ErrorPrefixes.QueryParsingError, $"Column '{columnName}' does not exist in cell '{cellName}'.");

            // 2. Prepare the new schema
            var newColumns = oldColumns.Where(c => c != columnToRemove).ToArray();

            // 3. Get the streaming enumerator for all cell instance data
            var allDataResult = indexManager.GetAllCellData(cellName);
            if (allDataResult.IsFailure) return allDataResult.Error;

            // 4. Migrate each row using a private helper method
            var migrationResult = await MigrateDroppedColumnData(cellName, oldColumns, newColumns, allDataResult.Value);
            if (migrationResult.IsFailure) return migrationResult.Error;

            // 5. After successful migration, update the environment file
            env.Columns = newColumns;
            return await SaveEnvironment(env, cellName);
        }

        private async Task<Result> MigrateDroppedColumnData(string cellName, Column[] oldColumns, Column[] newColumns, IEnumerable<KeyValuePair<byte[], byte[]>> allCellData)
        {
            foreach (var kvp in allCellData)
            {
                var instanceHash = kvp.Key;
                var rawData = kvp.Value;

                // a. Deserialize with the OLD schema
                var rowResult = RowSerializer.Deserialize(rawData, oldColumns);
                if (rowResult.IsFailure) return new Error(ErrorPrefixes.DataError, $"Failed to deserialize row during migration: {rowResult.Error.Message}");

                // b. Create a new row with the NEW schema
                var oldRow = rowResult.Value;
                var newValues = new object?[newColumns.Length];
                for (int i = 0; i < newColumns.Length; i++)
                {
                    newValues[i] = oldRow.Values[Array.IndexOf(oldColumns, newColumns[i])];
                }
                var newRow = Row.Create(newValues, newColumns);
                if(newRow.IsFailure) return newRow.Error;

                var newRowBytesResult = RowSerializer.Serialize(newRow.Value);
                if (newRowBytesResult.IsFailure) return new Error(ErrorPrefixes.DataError, $"Failed to serialize row during migration: {newRowBytesResult.Error.Message}");

                // d. Overwrite the data in the index
                var updateResult = indexManager.InsertCellData(cellName, instanceHash, newRowBytesResult.Value);
                if (updateResult.IsFailure) return new Error(ErrorPrefixes.DataError, $"Failed to write migrated row: {updateResult.Error.Message}");
            }
            return Result.Success();
        }
    }
}