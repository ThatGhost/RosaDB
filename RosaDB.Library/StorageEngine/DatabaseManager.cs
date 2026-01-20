using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine
{
    public class DatabaseManager(SessionState sessionState, IContextManager contextManager, IFileSystem fileSystem, IFolderManager folderManager) : IDatabaseManager
    {
        private readonly IFileSystem _fileSystem = fileSystem;
        private readonly IFolderManager _folderManager = folderManager;

        public Result<Database> GetDatabase(string databaseName)
        {
            var dbResult = Database.Create(databaseName);
            if (dbResult.IsFailure) return dbResult.Error;

            var database = dbResult.Value;
            var path = GetDatabasePath(database);

            if (!_folderManager.DoesFolderExist(path)) return new Error(ErrorPrefixes.DataError, $"Database '{databaseName}' not found.");

            return database;
        }

        public async Task<Result> CreateDatabaseEnvironment(Database database)
        {
            DatabaseEnvironment env = new DatabaseEnvironment();
            await SaveEnvironment(env, database);
            
            return Result.Success();
        }
        
        public async Task<Result> CreateContext(string contextName, Column[] columns)
        {
            try
            {
                if (sessionState.CurrentDatabase is null)
                    return new Error(ErrorPrefixes.StateError, "Database not set");

                var envResult = await GetEnvironment(sessionState.CurrentDatabase);
                if (!envResult.TryGetValue(out var env)) return envResult.Error;
                if (env.Contexts.Any(c => c.Name.Equals(contextName, StringComparison.OrdinalIgnoreCase)))
                    return new Error(ErrorPrefixes.FileError, $"Context '{contextName}' already exists in database '{sessionState.CurrentDatabase.Name}'.");

                var newContext = Context.Create(contextName);
                if (newContext.IsFailure) return newContext.Error;

                env.Contexts.Add(newContext.Value);
                await SaveEnvironment(env, sessionState.CurrentDatabase);

                _folderManager.CreateFolder(_fileSystem.Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), contextName));

                var contextEnvResult = await contextManager.CreateContextEnvironment(contextName, columns);
                if (contextEnvResult.IsFailure) return (await WipeContext(contextName)).IsFailure ? new CriticalError() : contextEnvResult.Error;
                
                return Result.Success();
            }
            catch
            {
                return (await WipeContext(contextName)).IsFailure ? new CriticalError() : new Error(ErrorPrefixes.FileError, "Context creation failed");
            }
        }
        
        public async Task<Result> DeleteContext(string contextName)
        {
            if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database not set");
            var envResult = await GetEnvironment(sessionState.CurrentDatabase);
            if (!envResult.TryGetValue(out var env)) return envResult.Error;
            
            var context = env.Contexts.FirstOrDefault(c => c.Name.Equals(contextName, StringComparison.OrdinalIgnoreCase));
            if (context == null) return new Error(ErrorPrefixes.FileError, "Context not found");
            
            string folderPath = _fileSystem.Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), contextName);
            string trashFolderPath = _fileSystem.Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), "trash_"+contextName);
            
            try { _folderManager.RenameFolder(folderPath, trashFolderPath); }
            catch { return new Error(ErrorPrefixes.FileError, "Could not prepare context for deletion (Folder Rename Failed)."); }
            
            env.Contexts.Remove(context);
            
            try { await SaveEnvironment(env, sessionState.CurrentDatabase); }
            catch 
            { 
                try
                {
                    env.Contexts.Add(context);
                    _folderManager.RenameFolder(trashFolderPath, folderPath);
                }
                catch { return new CriticalError(); }
                
                return new Error(ErrorPrefixes.FileError, "Failed to update database definition. Deletion reverted.");
            }

            try { _folderManager.DeleteFolder(trashFolderPath); }
            catch { return Result.Success(); } // TODO It is not integral to the database function that this is deleted. But need to add it to logging 
            
            return Result.Success();
        }
        
        private async Task<Result> WipeContext(string contextName)
        {
            try
            {
                if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database not set");
                var env = await GetEnvironment(sessionState.CurrentDatabase);
                
                if (env.IsSuccess) env.Value.Contexts.RemoveAll(c => c.Name == contextName);
                else return env.Error;
                
                await SaveEnvironment(env.Value, sessionState.CurrentDatabase);
                
                _folderManager.DeleteFolder(_fileSystem.Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), contextName));
                
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
