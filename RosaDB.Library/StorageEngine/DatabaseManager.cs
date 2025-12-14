using RosaDB.Library.Core;
using RosaDB.Library.Models;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Environment = RosaDB.Library.Models.DatabaseEnvironment;

namespace RosaDB.Library.StorageEngine
{
    public class DatabaseManager
    {
        private readonly string _databaseName;
        private readonly string _dbEnvFilePath;

        public DatabaseManager(string databaseName)
        {
            _databaseName = databaseName;
            _dbEnvFilePath = Path.Combine(FolderManager.BasePath, _databaseName, "_db_env");
        }

        public async Task<Result> CreateCell(string cellName, List<Column> columns)
        {
            var env = await GetEnvironment();
            if (env.Cells.Any(c => c.Name.Equals(cellName, StringComparison.OrdinalIgnoreCase)))
            {
                return new Error(ErrorPrefixes.FileError, $"Cell '{cellName}' already exists in database '{_databaseName}'.");
            }

            var newCell = new Cell(cellName, columns);
            env.Cells.Add(newCell);
            await SaveEnvironment(env);
            
            await FolderManager.CreateFolder(Path.Combine(_databaseName, cellName));

            return Result.Success();
        }

        private async Task<Environment> GetEnvironment()
        {
            if (!File.Exists(_dbEnvFilePath))
            {
                return new Environment();
            }

            var bytes = await ByteReaderWriter.ReadBytesFromFile(_dbEnvFilePath, CancellationToken.None);
            if (bytes.Length == 0)
            {
                return new Environment();
            }
            
            return ByteObjectConverter.ByteArrayToObject<Environment>(bytes) ?? new Environment();
        }

        private async Task SaveEnvironment(Environment env)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            await ByteReaderWriter.WriteBytesToFile(_dbEnvFilePath, bytes, CancellationToken.None);
        }
    }
}
