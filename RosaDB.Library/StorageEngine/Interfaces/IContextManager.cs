using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using System.Threading.Tasks;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface IContextManager
    {
        Task<Result> CreateContextEnvironment(string contextName, Column[] columns);
        Task<Result> UpdateContextEnvironment(string contextName, Column[] columns);
        Task<Result> CreateTable(string contextName, Table table);
        Task<Result> DeleteTable(string contextName, string tableName);
        Task<Result<ContextEnvironment>> GetEnvironment(string contextName);
        Task<Result<Column[]>> GetColumnsFromTable(string contextName, string tableName);
        Task<Result> CreateContextInstance(string cellGroupName, string instanceHash, Row instanceData, Column[] schema);
        Task<Result<Row>> GetContextInstance(string cellGroupName, string instanceHash);
        Task<Result<IEnumerable<Row>>> GetAllContextInstances(string cellGroupName);
        Task<Result> DropColumnAsync(string contextName, string columnName);
    }
}
