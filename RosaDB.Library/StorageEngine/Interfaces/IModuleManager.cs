using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using System.Threading.Tasks;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface IModuleManager
    {
        Task<Result> CreateModuleEnvironment(string contextName, Column[] columns);
        Task<Result> UpdateModuleEnvironment(string contextName, Column[] columns);
        Task<Result> CreateTable(string contextName, Table table);
        Task<Result> DeleteTable(string contextName, string tableName);
        Task<Result<ModuleEnvironment>> GetEnvironment(string contextName);
        Task<Result<Column[]>> GetColumnsFromTable(string contextName, string tableName);
        Result CreateModuleInstance(string contextName, string instanceHash, Row instanceData, Column[] schema);
        Task<Result<Row>> GetModuleInstance(string contextName, string instanceHash);
        Task<Result<IEnumerable<Row>>> GetAllModuleInstances(string contextName);
        Task<Result> DropColumnAsync(string contextName, string columnName);
    }
}
