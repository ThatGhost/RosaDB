using RosaDB.Library.Core;
using Environment = RosaDB.Library.Models.Environment;

namespace RosaDB.Library.StorageEngine
{
    public class RootManager
    {
        private readonly string EnvFilePath = Path.Combine(FolderManager.BasePath, "_env");

        public async Task<Result> CreateDatabase(string databaseName)
        {
            var env = await GetEnvironment();
            if (env.DatabaseNames.Contains(databaseName))
            {
                return new Error(ErrorPrefixes.FileError, $"Database '{databaseName}' already exists.");
            }

            env.DatabaseNames.Add(databaseName);
            await SaveEnvironment(env);
            
            await FolderManager.CreateFolder(databaseName);

            return Result.Success();
        }

        public async Task<List<string>> GetDatabaseNames()
        {
            var env = await GetEnvironment();
            return env.DatabaseNames;
        }

        private async Task<Environment> GetEnvironment()
        {
            if (!File.Exists(EnvFilePath))
            {
                return new Environment();
            }

            var bytes = await ByteReaderWriter.ReadBytesFromFile(EnvFilePath, CancellationToken.None);
            if (bytes.Length == 0)
            {
                return new Environment();
            }
            
            return ByteObjectConverter.ByteArrayToObject<Environment>(bytes) ?? new Environment();
        }

        private async Task SaveEnvironment(Environment env)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(EnvFilePath, bytes, CancellationToken.None);
        }
    }
}