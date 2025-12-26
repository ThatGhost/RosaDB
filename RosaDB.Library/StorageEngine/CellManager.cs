using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine
{
    public class CellManager(SessionState sessionState, IFileSystem fileSystem, IFolderManager folderManager) : ICellManager
    {
        private readonly IFileSystem _fileSystem = fileSystem;
        private readonly IFolderManager _folderManager = folderManager;
        private Dictionary<string,CellEnvironment> _cachedEnvironment = new(); 

        public async Task<Result> CreateCellEnvironment(string cellName, List<Column> columns)
        {
            CellEnvironment env = new CellEnvironment
            {
                Columns = columns.ToArray()
            };
            await SaveEnvironment(env, cellName);
            
            return Result.Success();
        }

        public async Task<Result> AddTables(string cellName, Table[] tables)
        {
            var env = await GetEnvironment(cellName);
            if (env.IsFailure) return env.Error;
            
            env.Value.Tables = env.Value.Tables.Concat(tables.ToArray()).ToArray();
            await SaveEnvironment(env.Value, cellName);

            if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database not set");

            foreach(var table in tables)
            {
                var tablePath = _fileSystem.Path.Combine(_folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, table.Name);
                try
                {
                    if(!_fileSystem.Directory.Exists(tablePath))
                    {
                        _fileSystem.Directory.CreateDirectory(tablePath);
                    }
                }
                catch (Exception ex)
                {
                    return new Error(ErrorPrefixes.FileError, $"Failed to create directory for table '{table.Name}': {ex.Message}");
                }
            }
            return Result.Success();
        }

        public async Task<Result> DeleteTable(string cellName, string tableName)
        {
            if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database not set");

            var envResult = await GetEnvironment(cellName);
            if (envResult.IsFailure) return envResult.Error;

            var tableToDelete = envResult.Value.Tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
            if (tableToDelete == null) return new Error(ErrorPrefixes.DataError, $"Table '{tableName}' not found in cell '{cellName}'.");

            string tablePath = _fileSystem.Path.Combine(_folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, tableName);
            string trashPath = _fileSystem.Path.Combine(_folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, "trash_" + tableName);

            try { await _folderManager.RenameFolder(tablePath, trashPath); }
            catch { return new Error(ErrorPrefixes.FileError, "Could not prepare table for deletion (Folder Rename Failed)."); }

            envResult.Value.Tables = envResult.Value.Tables.Where(t => t != tableToDelete).ToArray();

            try { await SaveEnvironment(envResult.Value, cellName); }
            catch
            {
                try
                {
                    await _folderManager.RenameFolder(trashPath, tablePath);
                    envResult.Value.Tables = envResult.Value.Tables.Append(tableToDelete).ToArray();
                }
                catch{ return new CriticalError(); }
                
                return new Error(ErrorPrefixes.FileError, "Failed to update cell definition. Deletion reverted.");
            }

            try { await _folderManager.DeleteFolder(trashPath); }
            catch { return Result.Success(); }

            return Result.Success();
        }

        public async Task<Result<CellEnvironment>> GetEnvironment(string cellName)
        {
            if (sessionState.CurrentDatabase is null)
                return new Error(ErrorPrefixes.StateError, "Database not set");

            if (_cachedEnvironment.TryGetValue(cellName, out var env)) return env;

            if (!_fileSystem.File.Exists(GetCellFilePath(cellName))) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(_fileSystem, GetCellFilePath(cellName), CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");
            
            env = ByteObjectConverter.ByteArrayToObject<CellEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            _cachedEnvironment[cellName] = env;
            return env;
        }

        private string GetCellFilePath(string cellName)
        {
            return _fileSystem.Path.Combine(_folderManager.BasePath, sessionState.CurrentDatabase!.Name, cellName, "_env");
        }
        
        private async Task SaveEnvironment(CellEnvironment env, string cellName)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(_fileSystem, GetCellFilePath(cellName), bytes, CancellationToken.None);
            _cachedEnvironment[cellName] = env;
        }
        
        public async Task<Result<Column[]>> GetColumnsFromTable(string cellName, string tableName)
        {
            var env = await GetEnvironment(cellName);
            if(env.IsFailure) return env.Error;

            var table = env.Value.Tables.FirstOrDefault(t => t.Name == tableName);
            if(table is null) return new Error(ErrorPrefixes.StateError, "Table does not exist in cell environment");

            return table.Columns
                .ToArray();
        }
    }
}
