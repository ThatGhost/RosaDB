using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;

namespace RosaDB.Library.StorageEngine
{
    public class DatabaseManager(SessionState sessionState)
    {
        public async Task<Result> CreateDatabaseEnvironment(Database database)
        {
            DatabaseEnvironment env = new DatabaseEnvironment();
            await SaveEnvironment(env, database);
            
            return Result.Success();
        }
        
        public async Task<Result> CreateCell(string cellName, List<Column> columns)
        {
            if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database not set");
            
            var env = await GetEnvironment(sessionState.CurrentDatabase);
            if (env.IsFailure) return env.Error!;
            if (env.Value.Cells.Any(c => c.Name.Equals(cellName, StringComparison.OrdinalIgnoreCase))) return new Error(ErrorPrefixes.FileError, $"Cell '{cellName}' already exists in database '{sessionState.CurrentDatabase.Name}'.");

            var newCell = new Cell(cellName, columns);
            env.Value.Cells.Add(newCell);
            await SaveEnvironment(env.Value, sessionState.CurrentDatabase);
            
            await FolderManager.CreateFolder(Path.Combine(GetDatabaseFilePath(sessionState.CurrentDatabase), cellName));

            return Result.Success();
        }

        private async Task<Result<DatabaseEnvironment>> GetEnvironment(Database database)
        {
            if (!File.Exists(GetDatabaseFilePath(database))) return new Error(ErrorPrefixes.FileError, "Database Environment does not exist");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(GetDatabaseFilePath(database), CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Database Environment does not exist");
            
            var env = ByteObjectConverter.ByteArrayToObject<DatabaseEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Database Environment does not exist");
            return env;
        }

        private string GetDatabaseFilePath(Database database)
        {
            return Path.Combine(FolderManager.BasePath, database.Name, "_env");
        }
        
        private async Task SaveEnvironment(DatabaseEnvironment env, Database database)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(GetDatabaseFilePath(database), bytes, CancellationToken.None);
        }
    }
}
