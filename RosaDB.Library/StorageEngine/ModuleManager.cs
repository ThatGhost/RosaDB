using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine
{
    public class ModuleManager(SessionState sessionState, IFileSystem fileSystem, IFolderManager folderManager, IIndexManager indexManager) : IModuleManager
    {
        private readonly Dictionary<string,ModuleEnvironment> _cachedEnvironment = new();

        public async Task<Result> CreateModuleEnvironment(string contextName, Column[] columns)
        {
            ModuleEnvironment env = new ModuleEnvironment
            {
                Columns = columns.ToArray()
            };
            return await SaveEnvironment(env, contextName);
        }

        public async Task<Result> UpdateModuleEnvironment(string contextName, Column[] columns)
        {
            var envResult = await GetEnvironment(contextName);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;

            env.Columns = columns;

            return await SaveEnvironment(env, contextName);
        }

        public Result CreateModuleInstance(string contextGroupName, string instanceHash, Row instanceData, Column[] schema)
        {
            // 1. Save main data
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);

            var existsResult = indexManager.ModuleDataExists(contextGroupName, hashBytes);
            if (existsResult.IsFailure) return existsResult.Error;
            if (existsResult.Value) return new Error(ErrorPrefixes.DataError, "Module instance already exists");
            
            var insertDataResult = indexManager.InsertModuleData(contextGroupName, hashBytes, instanceData.BSON);
            if (insertDataResult.IsFailure) return insertDataResult.Error;

            // 2. Update property indexes
            foreach (var col in schema.Where(c => c.IsIndex || c.IsPrimaryKey))
            {
                var value = instanceData.GetValue(col.Name);
                if (value == null) continue;
                
                var keyBytes = IndexKeyConverter.ToByteArray(value);
                var insertIndexResult = indexManager.InsertModulePropertyIndex(contextGroupName, col.Name, keyBytes, hashBytes);
                if (insertIndexResult.IsFailure) return insertIndexResult.Error;
            }
            return Result.Success();
        }

        public async Task<Result<Row>> GetModuleInstance(string contextGroupName, string instanceHash)
        {
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);

            var rowBytesResult = indexManager.GetModuleData(contextGroupName, hashBytes);
            if (!rowBytesResult.IsSuccess) return rowBytesResult.Error;
            
            var envResult = await GetEnvironment(contextGroupName);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;

            return RowSerializer.Deserialize(rowBytesResult.Value, env.Columns);

        }

        // TODO change to streaming of rows
        public async Task<Result<IEnumerable<Row>>> GetAllModuleInstances(string contextGroupName)
        {
            var allDataResult = indexManager.GetAllModuleData(contextGroupName);
            if (allDataResult.IsFailure) return allDataResult.Error;

            var envResult = await GetEnvironment(contextGroupName);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;

            var rows = new List<Row>();
            foreach (var kvp in allDataResult.Value)
            {
                var rowResult = RowSerializer.Deserialize(kvp.Value, env.Columns);
                if (rowResult.IsSuccess) rows.Add(rowResult.Value);
            }

            return rows;
        }

        public async Task<Result> CreateTable(string contextName, Table table)
        {
            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
            
            var env = await GetEnvironment(contextName);
            if (!env.TryGetValue(out var contextEnviroument)) return env.Error;

            var contextTables = contextEnviroument.Tables.ToList();
            contextTables.Add(table);
            contextEnviroument.Tables = contextTables.ToArray();
            
            var saveResult = await SaveEnvironment(contextEnviroument, contextName);
            if(saveResult.IsFailure) return saveResult.Error;
            
            var tablePath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, contextName, table.Name);

            try
            {
                if (!fileSystem.Directory.Exists(tablePath)) fileSystem.Directory.CreateDirectory(tablePath);
            }
            catch
            {
                // If you can't make folder, revert enviroument
                contextTables.Remove(table);
                contextEnviroument.Tables = contextTables.ToArray();
                var result = await SaveEnvironment(contextEnviroument, contextName);
                
                return result.IsFailure ? result.Error : new Error(ErrorPrefixes.FileError, $"Failed to create directory for table");
            }
            
            return Result.Success();
        }

        public async Task<Result> DeleteTable(string contextName, string tableName)
        {
            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();

            var envResult = await GetEnvironment(contextName);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;

            var tableToDelete = env.Tables.FirstOrDefault(t => t.Name.Equals(tableName));
            if (tableToDelete == null) return new Error(ErrorPrefixes.DataError, $"Table '{tableName}' not found in context '{contextName}'.");

            string tablePath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, contextName, tableName);
            string trashPath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, contextName, "trash_" + tableName);

            try { folderManager.RenameFolder(tablePath, trashPath); }
            catch { return new Error(ErrorPrefixes.FileError, "Could not prepare table for deletion (Folder Rename Failed)."); }

            env.Tables = env.Tables.Where(t => t != tableToDelete).ToArray();

            try
            {
                var result = await SaveEnvironment(env, contextName);
                if (result.IsFailure) return result.Error;
            }
            catch
            {
                try
                {
                    folderManager.RenameFolder(trashPath, tablePath);
                }
                catch{ return new CriticalError(); }
                
                return new Error(ErrorPrefixes.FileError, "Failed to update context definition. Deletion reverted.");
            }

            try { folderManager.DeleteFolder(trashPath); }
            catch { return Result.Success(); }

            return Result.Success();
        }

        public async Task<Result<ModuleEnvironment>> GetEnvironment(string contextName)
        {
            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();

            if (_cachedEnvironment.TryGetValue(contextName, out var env)) return env;

            var filePathResult = GetModuleFilePath(contextName, "_env");
            if(filePathResult.IsFailure) return filePathResult.Error;
            if (!fileSystem.File.Exists(filePathResult.Value)) return new Error(ErrorPrefixes.FileError, "Module Environment does not exist");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(fileSystem, filePathResult.Value, CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Module Environment does not exist");
            
            env = ByteObjectConverter.ByteArrayToObject<ModuleEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Module Environment does not exist");

            _cachedEnvironment[contextName] = env;
            return env;
        }
        
        public async Task<Result<Column[]>> GetColumnsFromTable(string contextName, string tableName)
        {
            var env = await GetEnvironment(contextName);
            if(env.IsFailure) return env.Error;

            var table = env.Value.Tables.FirstOrDefault(t => t.Name == tableName);
            if(table is null) return new Error(ErrorPrefixes.StateError, "Table does not exist in context environment");

            return table.Columns.ToArray();
        }

        public Task<Result> DropColumnAsync(string contextName, string columnName)
        {
            return GetEnvironment(contextName)
                .Then<ModuleEnvironment, (ModuleEnvironment, Column[], Column[])>(env =>
                {
                    var oldColumns = env.Columns;
                    var columnToRemove = oldColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                    if (columnToRemove == null)
                        return new Error(ErrorPrefixes.QueryParsingError, $"Column '{columnName}' does not exist in context '{contextName}'.");
        
                    var newColumns = oldColumns.Where(c => c != columnToRemove).ToArray();
                    return (env, oldColumns, newColumns);
                })
                .Then<(ModuleEnvironment, Column[], Column[]), (ModuleEnvironment, Column[], Column[], IEnumerable<KeyValuePair<byte[], byte[]>>)>(data =>
                { 
                    var contextData = indexManager.GetAllModuleData(contextName);
                    if (contextData.IsFailure) return contextData.Error;
                    return (data.Item1, data.Item2, data.Item3, contextData.Value);
                })
                .ThenAsync<(ModuleEnvironment, Column[], Column[], IEnumerable<KeyValuePair<byte[], byte[]>>), (ModuleEnvironment, Column[])>(async data =>
                {
                    var migrationResult = await MigrateDroppedColumnData(contextName, data.Item2, data.Item3, data.Item4);
                    return migrationResult.IsSuccess ? (data.Item1, data.Item3) : migrationResult.Error;
                })
                .ThenAsync(async data =>
                {
                    var env = data.Item1;
                    var newColumns = data.Item2;
                    env.Columns = newColumns;
                    return await SaveEnvironment(env, contextName);
                });
        }
        
        private Result<string> GetModuleFilePath(string contextName, string fileName)
        {
            if(sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
            return fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, contextName, fileName);
        }
        
        private async Task<Result> SaveEnvironment(ModuleEnvironment env, string contextName)
        {
            var filePath = GetModuleFilePath(contextName, "_env");
            if(filePath.IsFailure) return filePath.Error;

            var bytes = ByteObjectConverter.ObjectToByteArray(env);

            await ByteReaderWriter.WriteBytesToFile(fileSystem, filePath.Value, bytes, CancellationToken.None);
            _cachedEnvironment[contextName] = env;
            return Result.Success();
        }

        private async Task<Result> MigrateDroppedColumnData(string contextName, Column[] oldColumns, Column[] newColumns, IEnumerable<KeyValuePair<byte[], byte[]>> allModuleData)
        {
            foreach (var kvp in allModuleData)
            {
                var migrationResult = 
                    RowSerializer.Deserialize(kvp.Value, oldColumns)
                    .Then(oldRow =>
                    {
                        var newValues = new object?[newColumns.Length];
                        for (int i = 0; i < newColumns.Length; i++)
                        {
                            newValues[i] = oldRow.Values[Array.IndexOf(oldColumns, newColumns[i])];
                        }
                        return Row.Create(newValues, newColumns);
                    })
                    .Finally(row => indexManager.InsertModuleData(contextName, kvp.Key, row.BSON));

                if (migrationResult.IsFailure) return migrationResult.Error;
            }
            return Result.Success();
        }
    }
}