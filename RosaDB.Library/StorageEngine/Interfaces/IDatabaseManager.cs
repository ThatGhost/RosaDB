using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface IDatabaseManager
    {
        Result<Database> GetDatabase(string name);
        Task<Result> CreateDatabase(Database database);
        Task<Result> CreateModule(string module, Column[] columns);
        Task<Result> DeleteModule(string module);
        Task<Result> CreateTable(string module, string tableName, Column[] columns);
        Task<Result> DeleteTable(string tableName);
    }
}
