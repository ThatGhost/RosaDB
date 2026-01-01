using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;

namespace RosaDB.Library.StorageEngine
{
    public class CellManager(SessionState sessionState, IFileSystem fileSystem, IFolderManager folderManager) : ICellManager
    {
        private readonly IFileSystem _fileSystem = fileSystem;
        private readonly IFolderManager _folderManager = folderManager;
        private readonly Dictionary<string,CellEnvironment> _cachedEnvironment = new();
        private readonly Dictionary<string, BPlusTree<byte[], byte[]>> _activeCellInstanceStores = new();
        private readonly Dictionary<string, BPlusTree<byte[], byte[]>> _activeCellPropertyIndexes = new();

        public async Task<Result> CreateCellEnvironment(string cellName, Column[] columns)
        {
            CellEnvironment env = new CellEnvironment
            {
                Columns = columns.ToArray()
            };
            await SaveEnvironment(env, cellName);
            
            return Result.Success();
        }

        public async Task<Result> CreateCellInstance(string cellGroupName, string instanceHash, Row instanceData, Column[] schema)
        {
            // 1. Save main data
            var mainStoreResult = GetOrCreateInstanceStore(cellGroupName);
            if (!mainStoreResult.TryGetValue(out var mainStore)) return mainStoreResult.Error;

            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);

            var rowBytesResult = RowSerializer.Serialize(instanceData);
            if (!rowBytesResult.TryGetValue(out var rowBytes))
                return rowBytesResult.Error;

            mainStore[hashBytes] = rowBytes;
            mainStore.Commit();

            // 2. Update property indexes
            foreach (var col in schema.Where(c => c.IsIndex || c.IsPrimaryKey))
            {
                var value = instanceData[col.Name];
                if (value != null)
                {
                    var propIndexResult = GetOrCreatePropertyIndex(cellGroupName, col.Name);
                    if (!propIndexResult.TryGetValue(out var propIndex)) return propIndexResult.Error;
                    var keyBytes = IndexKeyConverter.ToByteArray(value);
                    propIndex[keyBytes] = hashBytes;
                    propIndex.Commit();
                }
            }
            return Result.Success();
        }

        public async Task<Result<Row>> GetCellInstance(string cellGroupName, string instanceHash)
        {
            var mainStoreResult = GetOrCreateInstanceStore(cellGroupName);
            if (!mainStoreResult.TryGetValue(out var mainStore)) return mainStoreResult.Error;

            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);

            if (mainStore.TryGetValue(hashBytes, out var rowBytes))
            {
                var envResult = await GetEnvironment(cellGroupName);
                if (!envResult.TryGetValue(out var env)) return envResult.Error;

                var rowResult = RowSerializer.Deserialize(rowBytes, env.Columns);
                return rowResult;
            }

            return new Error(ErrorPrefixes.DataError, "Cell instance not found.");
        }

        public async Task<Result> CreateTable(string cellName, Table table)
        {
            var env = await GetEnvironment(cellName);
            if (!env.TryGetValue(out var cellEnviroument)) return env.Error;

            var cellTables = cellEnviroument.Tables.ToList();
            cellTables.Add(table);
            cellEnviroument.Tables = cellTables.ToArray();
            await SaveEnvironment(cellEnviroument, cellName);

            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();

            var tablePath = _fileSystem.Path.Combine(_folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, table.Name);
            try
            {
                if(!_fileSystem.Directory.Exists(tablePath)) _fileSystem.Directory.CreateDirectory(tablePath);
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

            string tablePath = _fileSystem.Path.Combine(_folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, tableName);
            string trashPath = _fileSystem.Path.Combine(_folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, "trash_" + tableName);

            try { _folderManager.RenameFolder(tablePath, trashPath); }
            catch { return new Error(ErrorPrefixes.FileError, "Could not prepare table for deletion (Folder Rename Failed)."); }

            env.Tables = env.Tables.Where(t => t != tableToDelete).ToArray();

            try { await SaveEnvironment(env, cellName); }
            catch
            {
                try
                {
                    _folderManager.RenameFolder(trashPath, tablePath);
                    envResult.Value.Tables = envResult.Value.Tables.Append(tableToDelete).ToArray();
                }
                catch{ return new CriticalError(); }
                
                return new Error(ErrorPrefixes.FileError, "Failed to update cell definition. Deletion reverted.");
            }

            try { _folderManager.DeleteFolder(trashPath); }
            catch { return Result.Success(); }

            return Result.Success();
        }

        public async Task<Result<CellEnvironment>> GetEnvironment(string cellName)
        {
            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();

            if (_cachedEnvironment.TryGetValue(cellName, out var env)) return env;

            var filePathResult = GetCellFilePath(cellName, "_env");
            if(filePathResult.IsFailure) return filePathResult.Error;
            if (!_fileSystem.File.Exists(filePathResult.Value)) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(_fileSystem, filePathResult.Value, CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");
            
            env = ByteObjectConverter.ByteArrayToObject<CellEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            _cachedEnvironment[cellName] = env;
            return env;
        }

        private Result<BPlusTree<byte[], byte[]>> GetOrCreateInstanceStore(string cellGroupName)
        {
            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();

            var key = $"{sessionState.CurrentDatabase.Name}_{cellGroupName}";
            if (_activeCellInstanceStores.TryGetValue(key, out var tree)) return tree;

            var filePath = GetCellFilePath(cellGroupName, "_idx");
            if(filePath.IsFailure) return filePath.Error;

            var options = new BPlusTree<byte[], byte[]>.OptionsV2(PrimitiveSerializer.Bytes, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = filePath.Value
            };
            var newTree = new BPlusTree<byte[], byte[]>(options);
            _activeCellInstanceStores[key] = newTree;
            return newTree;
        }
        
        private Result<BPlusTree<byte[], byte[]>> GetOrCreatePropertyIndex(string cellGroupName, string propertyName)
        {
            if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();

            var key = $"{sessionState.CurrentDatabase!.Name}_{cellGroupName}_{propertyName}";
            if (_activeCellPropertyIndexes.TryGetValue(key, out var tree)) return tree;

            var filePath = GetCellFilePath(cellGroupName, $"_pidx_{propertyName}");
            if(filePath.IsFailure) return filePath.Error;

            var options = new BPlusTree<byte[], byte[]>.OptionsV2(PrimitiveSerializer.Bytes, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = filePath.Value
            };
            var newTree = new BPlusTree<byte[], byte[]>(options);
            _activeCellPropertyIndexes[key] = newTree;
            return newTree;
        }

        private Result<string> GetCellFilePath(string cellName, string fileName)
        {
            if(sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
            return _fileSystem.Path.Combine(_folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, fileName);
        }
        
        private async Task<Result> SaveEnvironment(CellEnvironment env, string cellName)
        {
            var filePath = GetCellFilePath(cellName, "_env");
            if(filePath.IsFailure) return filePath.Error;

            var bytes = ByteObjectConverter.ObjectToByteArray(env);

            await ByteReaderWriter.WriteBytesToFile(_fileSystem, filePath.Value, bytes, CancellationToken.None);
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

        public void CloseAll()
        {
            foreach (var tree in _activeCellInstanceStores.Values)
                tree.Dispose();
            foreach (var tree in _activeCellPropertyIndexes.Values)
                tree.Dispose();
            _activeCellInstanceStores.Clear();
            _activeCellPropertyIndexes.Clear();
        }
    }
}
