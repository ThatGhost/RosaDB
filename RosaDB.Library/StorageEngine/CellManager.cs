using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;

namespace RosaDB.Library.StorageEngine
{
    public class CellManager(SessionState sessionState)
    {
        private Dictionary<string,CellEnvironment> _cachedEnvironment = new(); 

        public async Task<Result> CreateCellEnvironment(Cell cell, List<Column> columns)
        {
            CellEnvironment env = new CellEnvironment
            {
                Columns = columns.ToArray()
            };
            await SaveEnvironment(env, cell);
            
            return Result.Success();
        }

        public async Task<Result> AddTables(Cell cell, Table[] tables)
        {
            var env = await GetEnvironment(cell);
            if (env.IsFailure) return env.Error!;
            
            env.Value.Tables = env.Value.Tables.Concat(tables.ToArray()).ToArray();
            await SaveEnvironment(env.Value, cell);
            return Result.Success();
        }

        public async Task<Result<CellEnvironment>> GetEnvironment(Cell cell)
        {
            if (sessionState.CurrentDatabase is null)
                return new Error(ErrorPrefixes.StateError, "Database not set");

            if (_cachedEnvironment.TryGetValue(cell.Name, out var env)) return env;

            if (!File.Exists(GetCellFilePath(cell))) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(GetCellFilePath(cell), CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");
            
            env = ByteObjectConverter.ByteArrayToObject<CellEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            _cachedEnvironment[cell.Name] = env;
            return env;
        }

        private string GetCellFilePath(Cell cell)
        {
            return Path.Combine(FolderManager.BasePath, sessionState.CurrentDatabase!.Name, cell.Name, "_env");
        }
        
        private async Task SaveEnvironment(CellEnvironment env, Cell cell)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(GetCellFilePath(cell), bytes, CancellationToken.None);
            _cachedEnvironment[cell.Name] = env;
        }

        public async Task<Result<DataType[]>> GetDataTypesFromTableCollums(string cellName, string tableName, string[] collumNames)
        {
            Cell cell = new Cell(cellName);
            var env = await GetEnvironment(cell);
            if(env.IsFailure) return env.Error!;

            var table = env.Value.Tables.FirstOrDefault(t => t.Name == tableName);
            if(table is null) return new Error(ErrorPrefixes.StateError, "Table does not exist in cell environment");

            return table.Columns
                .Where(c => collumNames.Contains(c.Name))
                .Select(c => c.DataType)
                .ToArray();
        }
    }
}
