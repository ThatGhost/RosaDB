using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface IDatabaseManager
    {
        Result<Database> GetDatabase(string databaseName);
        Task<Result> CreateDatabaseEnvironment(Database database);
        Task<Result> CreateModule(string moduleName, Column[] columns);
        Task<Result> DeleteModule(string moduleName);
    }
}
