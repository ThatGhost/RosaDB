using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.StorageEngine
{
    public class CellManager(SessionState sessionState)
    {
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
            return Result.Success();
        }

        public async Task<Result<CellEnvironment>> GetEnvironment(string cellName)
        {
            if (sessionState.CurrentDatabase is null)
                return new Error(ErrorPrefixes.StateError, "Database not set");

            if (_cachedEnvironment.TryGetValue(cellName, out var env)) return env;

            if (!File.Exists(GetCellFilePath(cellName))) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(GetCellFilePath(cellName), CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");
            
            env = ByteObjectConverter.ByteArrayToObject<CellEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            _cachedEnvironment[cellName] = env;
            return env;
        }

        private string GetCellFilePath(string cellName)
        {
            return Path.Combine(FolderManager.BasePath, sessionState.CurrentDatabase!.Name, cellName, "_env");
        }
        
        private async Task SaveEnvironment(CellEnvironment env, string cellName)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(GetCellFilePath(cellName), bytes, CancellationToken.None);
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
