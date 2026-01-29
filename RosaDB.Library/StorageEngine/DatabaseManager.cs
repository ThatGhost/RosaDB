using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine
{
    public class DatabaseManager(SessionState sessionState, IFileSystem fileSystem, IFolderManager folderManager) : IDatabaseManager
    {
        public Result<Database> GetDatabase(string databaseName)
        {
            if(!folderManager.DoesFolderExist(databaseName)) return new Error(ErrorPrefixes.FileError, "Database not found");
            
        }

        public async Task<Result> CreateDatabase(Database database)
        {
            
            
            return Result.Success();
        }
        
        public async Task<Result> CreateModule(string module, Column[] columns)
        {
            
        }
        
        public async Task<Result> DeleteModule(string name)
        {
            
        }

        public Task<Result> CreateTable(string module, string tableName, Column[] columns)
        {
            throw new NotImplementedException();
        }

        public Task<Result> DeleteTable(string tableName)
        {
            throw new NotImplementedException();
        }
    }
}
