using RosaDB.Library.Core;
using RosaDB.Library.Models;
using Environment = RosaDB.Library.Models.CellEnvironment;

namespace RosaDB.Library.StorageEngine
{
    public class CellManager
    {
        private readonly string _databaseName;
        private readonly string _cellName;
        private readonly string _cellEnvFilePath;

        public CellManager(string databaseName, string cellName)
        {
            _databaseName = databaseName;
            _cellName = cellName;
            _cellEnvFilePath = Path.Combine(FolderManager.BasePath, _databaseName, _cellName, "_cell_env");
        }

        public async Task<Result> CreateTable(string tableName)
        {
            var env = await GetEnvironment();
            if (env.Tables.Any(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase)))
            {
                return new Error(ErrorPrefixes.FileError, $"Table '{tableName}' already exists in cell '{_cellName}'.");
            }

            var newTable = new Table() { Name = tableName };
            env.Tables.Add(newTable);
            await SaveEnvironment(env);
            
            // For now, just create an empty file for the table
            await FolderManager.CreateFile(Path.Combine(_databaseName, _cellName, $"{tableName}.rdb"));

            return Result.Success();
        }

        private async Task<Environment> GetEnvironment()
        {
            if (!File.Exists(_cellEnvFilePath))
            {
                return new Environment();
            }

            var bytes = await ByteReaderWriter.ReadBytesFromFile(_cellEnvFilePath, CancellationToken.None);
            if (bytes.Length == 0)
            {
                return new Environment();
            }
            
            return ByteObjectConverter.ByteArrayToObject<Environment>(bytes) ?? new Environment();
        }

        private async Task SaveEnvironment(Environment env)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(_cellEnvFilePath, bytes, CancellationToken.None);
        }
    }
}
