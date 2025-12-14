using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;

namespace RosaDB.Library.StorageEngine
{
    public class DatabaseManager(SessionState sessionState, CellManager cellManager)
    {
        public async Task<Result> CreateDatabaseEnvironment(Database database)
        {
            DatabaseEnvironment env = new DatabaseEnvironment();
            await SaveEnvironment(env, database);
            
            return Result.Success();
        }
        
        public async Task<Result> CreateCell(string cellName, List<Column> columns)
        {
            try
            {
                if (sessionState.CurrentDatabase is null)
                    return new Error(ErrorPrefixes.StateError, "Database not set");

                var env = await GetEnvironment(sessionState.CurrentDatabase);
                if (env.IsFailure) return env.Error!;
                if (env.Value.Cells.Any(c => c.Name.Equals(cellName, StringComparison.OrdinalIgnoreCase)))
                    return new Error(ErrorPrefixes.FileError, $"Cell '{cellName}' already exists in database '{sessionState.CurrentDatabase.Name}'.");

                var newCell = new Cell(cellName);
                env.Value.Cells.Add(newCell);
                await SaveEnvironment(env.Value, sessionState.CurrentDatabase);

                await FolderManager.CreateFolder(Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), cellName));

                var cellEnvResult = await cellManager.CreateCellEnvironment(newCell, columns);
                if (cellEnvResult.IsFailure) return (await WipeCell(cellName)).IsFailure ? new CriticalError() : cellEnvResult.Error!;
                
                return Result.Success();
            }
            catch
            {
                return (await WipeCell(cellName)).IsFailure ? new CriticalError() : new Error(ErrorPrefixes.FileError, "Cell creation failed");
            }
        }

        // Need to add atomicity
        public async Task<Result> DeleteCell(string cellName)
        {
            try
            {
                if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database not set");
                var env = await GetEnvironment(sessionState.CurrentDatabase);
                
                var cell = env.Value.Cells.FirstOrDefault(c => c.Name.Equals(cellName, StringComparison.OrdinalIgnoreCase));
                if (cell == null) return new Error(ErrorPrefixes.FileError, "Cell not found");
                
                env.Value.Cells.Remove(cell);
                
                await FolderManager.DeleteFolder(Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), cellName));
                
                return Result.Success();
            }
            catch { return new Error(ErrorPrefixes.FileError, "Something went wrong"); }
        }
        
        private async Task<Result> WipeCell(string cellName)
        {
            try
            {
                if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database not set");
                var env = await GetEnvironment(sessionState.CurrentDatabase);
                if(env.IsSuccess) env.Value.Cells.RemoveAll(c => c.Name == cellName);
                await SaveEnvironment(env.Value, sessionState.CurrentDatabase);
                
                await FolderManager.DeleteFolder(Path.Combine(GetDatabasePath(sessionState.CurrentDatabase), cellName));
                
                return Result.Success();
            }
            catch { return new CriticalError(); }
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

        private string GetDatabasePath(Database database) => Path.Combine(FolderManager.BasePath, database.Name);

        private string GetDatabaseFilePath(Database database)
        {
            return Path.Combine(GetDatabasePath(database), "_env");
        }
        
        private async Task SaveEnvironment(DatabaseEnvironment env, Database database)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(GetDatabaseFilePath(database), bytes, CancellationToken.None);
        }
    }
}
