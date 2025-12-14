using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;

namespace RosaDB.Library.StorageEngine
{
    public class RootManager(DatabaseManager databaseManager)
    {
        private readonly string EnvFilePath = Path.Combine(FolderManager.BasePath, "_env");

        public async Task<Result> CreateDatabase(string databaseName)
        {
            try
            {
                var env = await GetEnvironment();
                if (env.IsFailure) return new Error(ErrorPrefixes.FileError, "RosaDb not setup correctly");
                if (env.Value.DatabaseNames.Contains(databaseName)) return new Error(ErrorPrefixes.FileError, $"Database '{databaseName}' already exists.");

                env.Value.DatabaseNames.Add(databaseName);
                await SaveEnvironment(env.Value);

                await FolderManager.CreateFolder(databaseName);
                var envResult = await databaseManager.CreateDatabaseEnvironment(new Database(databaseName));
                if (envResult.IsFailure)
                {
                    return (await WipeDatabase(databaseName)).IsFailure ? new CriticalError() : envResult.Error!;
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
            try
            {
                var result = await GetEnvironment();
                if (result.IsSuccess) return new Error(ErrorPrefixes.StateError, "Root already setup");
                
                RootEnvironment env = new RootEnvironment();
                await SaveEnvironment(env);
                
                return Result.Success();
            }
            catch { return new Error(ErrorPrefixes.FileError, "RosaDb not setup correctly"); }
        }
        
        // Atomicity
        private async Task<Result> WipeDatabase(string databaseName)
        {
            try
            {
                var env = await GetEnvironment();
                if (env.IsFailure) return new CriticalError();
            
                env.Value.DatabaseNames.Remove(databaseName);
                await SaveEnvironment(env.Value);
            
                await FolderManager.DeleteFolder(databaseName);
            
                return Result.Success();
            }
            catch { return new CriticalError(); }
        }

        public async Task<Result<List<string>>> GetDatabaseNames()
        {
            var env = await GetEnvironment();
            return env.IsSuccess ? env.Value.DatabaseNames : new Error(ErrorPrefixes.FileError, "Databases not found.");
        }

        private async Task<Result<RootEnvironment>> GetEnvironment()
        {
            if (!File.Exists(EnvFilePath)) return new Error(ErrorPrefixes.FileError, "Database environment file not found.");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(EnvFilePath, CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Database environment file empty");
            
            var env = ByteObjectConverter.ByteArrayToObject<RootEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Database environment file empty");
            return env;
        }

        private async Task SaveEnvironment(RootEnvironment env)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(EnvFilePath, bytes, CancellationToken.None);
        }
    }
}