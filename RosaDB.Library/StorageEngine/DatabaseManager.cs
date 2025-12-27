using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine
{
    public class DatabaseManager(SessionState sessionState, ICellManager cellManager, IFileSystem fileSystem, IFolderManager folderManager) : IDatabaseManager
    {
        private readonly IFileSystem _fileSystem = fileSystem;
        private readonly IFolderManager _folderManager = folderManager;

        public async Task<Result> CreateDatabaseEnvironment(Database database)
        {
            DatabaseEnvironment env = new DatabaseEnvironment();
            await SaveEnvironment(env, database);
            
            return Result.Success();
        }
        
        public async Task<Result> CreateCell(string cellName, Column[] columns)
        {
            try
            {
                if (sessionState.CurrentDatabase is null)
                    return new Error(ErrorPrefixes.StateError, "Database not set");

                var envResult = await GetEnvironment(sessionState.CurrentDatabase);
                if (!envResult.TryGetValue(out var env)) return envResult.Error;
                if (env.Cells.Any(c => c.Name.Equals(cellName, StringComparison.OrdinalIgnoreCase)))
                    return new Error(ErrorPrefixes.FileError, $"Cell '{cellName}' already exists in database '{sessionState.CurrentDatabase.Name}'.");

                var newCell = Cell.Create(cellName);
                if (newCell.IsFailure) return newCell.Error;

                env.Cells.Add(newCell.Value);
                await SaveEnvironment(env, sessionState.CurrentDatabase);

                _folderManager.CreateFolder(_fileSystem.Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), cellName));

                var cellEnvResult = await cellManager.CreateCellEnvironment(cellName, columns);
                if (cellEnvResult.IsFailure) return (await WipeCell(cellName)).IsFailure ? new CriticalError() : cellEnvResult.Error;
                
                return Result.Success();
            }
            catch
            {
                return (await WipeCell(cellName)).IsFailure ? new CriticalError() : new Error(ErrorPrefixes.FileError, "Cell creation failed");
            }
        }
        
        public async Task<Result> DeleteCell(string cellName)
        {
            if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database not set");
            var envResult = await GetEnvironment(sessionState.CurrentDatabase);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;
            
            var cell = env.Cells.FirstOrDefault(c => c.Name.Equals(cellName, StringComparison.OrdinalIgnoreCase));
            if (cell == null) return new Error(ErrorPrefixes.FileError, "Cell not found");
            
            string folderPath = _fileSystem.Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), cellName);
            string trashFolderPath = _fileSystem.Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), "trash_"+cellName);
            
            try { _folderManager.RenameFolder(folderPath, trashFolderPath); }
            catch { return new Error(ErrorPrefixes.FileError, "Could not prepare cell for deletion (Folder Rename Failed)."); }
            
            env.Cells.Remove(cell);
            
            try { await SaveEnvironment(env, sessionState.CurrentDatabase); }
            catch 
            { 
                try
                {
                    env.Cells.Add(cell);
                    _folderManager.RenameFolder(trashFolderPath, folderPath);
                }
                catch { return new CriticalError(); }
                
                return new Error(ErrorPrefixes.FileError, "Failed to update database definition. Deletion reverted.");
            }

            try { _folderManager.DeleteFolder(trashFolderPath); }
            catch { return Result.Success(); } // TODO It is not integral to the database function that this is deleted. But need to add it to logging 
            
            return Result.Success();
        }
        
        private async Task<Result> WipeCell(string cellName)
        {
            try
            {
                if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database not set");
                var env = await GetEnvironment(sessionState.CurrentDatabase);
                
                if (env.IsSuccess) env.Value.Cells.RemoveAll(c => c.Name == cellName);
                else return env.Error;
                
                await SaveEnvironment(env.Value, sessionState.CurrentDatabase);
                
                _folderManager.DeleteFolder(_fileSystem.Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), cellName));
                
                return Result.Success();
            }
            catch { return new CriticalError(); }
        }

        private async Task<Result<DatabaseEnvironment>> GetEnvironment(Database database)
        {
            if (!_fileSystem.File.Exists(GetDatabaseFilePath(database))) return new Error(ErrorPrefixes.FileError, "Database Environment does not exist");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(_fileSystem, GetDatabaseFilePath(database), CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Database Environment does not exist");
            
            var env = ByteObjectConverter.ByteArrayToObject<DatabaseEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Database Environment does not exist");
            return env;
        }

        private string GetDatabasePath(Database database) => _fileSystem.Path.Combine(_folderManager.BasePath, database.Name);

        private string GetDatabaseFilePath(Database database)
        {
            return _fileSystem.Path.Combine(GetDatabasePath(database), "_env");
        }
        
        private async Task SaveEnvironment(DatabaseEnvironment env, Database database)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(_fileSystem, GetDatabaseFilePath(database), bytes, CancellationToken.None);
        }
    }
}
