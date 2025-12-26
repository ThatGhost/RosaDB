#nullable disable

using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine
{
    public class RootManager(IDatabaseManager databaseManager, SessionState sessionState, IFileSystem fileSystem, IFolderManager folderManager)
    {
        private readonly IFileSystem _fileSystem = fileSystem;
        private readonly IFolderManager _folderManager = folderManager;
        private readonly string EnvFilePath = fileSystem.Path.Combine(folderManager.BasePath, "_env");

        public async Task<Result> CreateDatabase(string databaseName)
        {
            try
            {
                var env = await GetEnvironment();
                if (env.IsFailure) return new Error(ErrorPrefixes.FileError, "RosaDb not setup correctly");
                if (env.Value.DatabaseNames.Contains(databaseName)) return new Error(ErrorPrefixes.FileError, $"Database '{databaseName}' already exists.");

                env.Value.DatabaseNames.Add(databaseName);
                await SaveEnvironment(env.Value);
                var database = Database.Create(databaseName);
                if (database.IsFailure) return database.Error;
                    
                await _folderManager.CreateFolder(databaseName);
                var envResult = await databaseManager.CreateDatabaseEnvironment(database.Value);
                if (envResult.IsFailure)
                {
                    return (await WipeDatabase(databaseName)).IsFailure ? new CriticalError() : envResult.Error;
                }

                return Result.Success();
            }
            catch
            {
                return (await WipeDatabase(databaseName)).IsFailure ? new CriticalError() : new Error(ErrorPrefixes.FileError, "Database creation failed");
            }
        }

        public async Task<Result> InitializeRoot()
        {
            var result = await GetEnvironment();
            if (result.IsSuccess)
            {
                return new Error(ErrorPrefixes.StateError, "Root already setup");
            }
            
            RootEnvironment env = new RootEnvironment();
            await SaveEnvironment(env);
            
            return Result.Success();
        }
        
        public async Task<Result> WipeDatabase(string databaseName)
        {
            try
            {
                var env = await GetEnvironment();
                if (env.IsFailure) return new CriticalError();
            
                env.Value.DatabaseNames.Remove(databaseName);
                await SaveEnvironment(env.Value);
            
                await _folderManager.DeleteFolder(databaseName);
            
                return Result.Success();
            }
            catch { return new CriticalError(); }
        }

        public async Task<Result> DeleteDatabase(string databaseName)
        {
            var envResult = await GetEnvironment();
            if (envResult.IsFailure) return envResult.Error;

            if (!envResult.Value.DatabaseNames.Contains(databaseName)) return new Error(ErrorPrefixes.DataError, $"Database '{databaseName}' not found.");

            string folderPath = _fileSystem.Path.Combine(_folderManager.BasePath, databaseName);
            string trashFolderPath = _fileSystem.Path.Combine(_folderManager.BasePath, "trash_" + databaseName);

            try { await _folderManager.RenameFolder(folderPath, trashFolderPath); }
            catch { return new Error(ErrorPrefixes.FileError, "Could not prepare database for deletion (Folder Rename Failed)."); }

            envResult.Value.DatabaseNames.Remove(databaseName);

            try { await SaveEnvironment(envResult.Value); }
            catch
            {
                try
                {
                    await _folderManager.RenameFolder(trashFolderPath, folderPath);
                    envResult.Value.DatabaseNames.Add(databaseName); 
                }
                catch { return new CriticalError(); }
                
                return new Error(ErrorPrefixes.FileError, "Failed to update root definition. Deletion reverted.");
            }

            if (sessionState.CurrentDatabase is not null && sessionState.CurrentDatabase.Name == databaseName)
                sessionState.CurrentDatabase = null;

            try { await _folderManager.DeleteFolder(trashFolderPath); }
            catch { return Result.Success(); }

            return Result.Success();
        }

        public async Task<Result<List<string>>> GetDatabaseNames()
        {
            var env = await GetEnvironment();
            return env.IsSuccess ? env.Value.DatabaseNames : new Error(ErrorPrefixes.FileError, "Databases not found.");
        }

        private async Task<Result<RootEnvironment>> GetEnvironment()
        {
            if (!_fileSystem.File.Exists(EnvFilePath)) return new Error(ErrorPrefixes.FileError, "Database environment file not found.");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(_fileSystem, EnvFilePath, CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Database environment file empty");
            
            var env = ByteObjectConverter.ByteArrayToObject<RootEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Database environment file empty");
            return env;
        }

        private async Task SaveEnvironment(RootEnvironment env)
        {
            var directory = _fileSystem.Path.GetDirectoryName(EnvFilePath);
            if (!_fileSystem.Directory.Exists(directory))
            {
                _fileSystem.Directory.CreateDirectory(directory);
            }

            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(_fileSystem, EnvFilePath, bytes, CancellationToken.None);
        }
    }
}