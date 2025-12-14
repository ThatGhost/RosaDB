using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;

namespace RosaDB.Library.StorageEngine
{
    public class CellManager(SessionState sessionState)
    {
        public async Task<Result> CreateCellEnvironment(Cell cell, List<Column> columns)
        {
            CellEnvironment env = new CellEnvironment
            {
                Columns = columns.ToArray()
            };
            await SaveEnvironment(env, cell);
            
            return Result.Success();
        }

        private async Task<Result<CellEnvironment>> GetEnvironment(Cell cell)
        {
            if (sessionState.CurrentDatabase is null)
                return new Error(ErrorPrefixes.StateError, "Database not set");

            if (!File.Exists(GetCellFilePath(cell))) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");

            var bytes = await ByteReaderWriter.ReadBytesFromFile(GetCellFilePath(cell), CancellationToken.None);
            if (bytes.Length == 0) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");
            
            var env = ByteObjectConverter.ByteArrayToObject<CellEnvironment>(bytes);
            if(env is null) return new Error(ErrorPrefixes.FileError, "Cell Environment does not exist");
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
        }
    }
}
