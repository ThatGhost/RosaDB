using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface IDatabaseManager
    {
        Task<Result<Database>> GetDatabase(string name);
        Task<Result> CreateDatabase(Database database);
        Task<Result> CreateModule(Module module);
        Task<Result> DeleteModule(string module);
        Task<Result<Module>> GetModule(string moduleName);
        Task<Result> CreateTable(string module, Table table);
        Task<Result> DeleteTable(string moduleName, string tableName);
        Task<Result<Table>> GetTable(string moduleName, string tableName);
    }
}
