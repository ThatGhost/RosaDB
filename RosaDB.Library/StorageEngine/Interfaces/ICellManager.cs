using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using System.Threading.Tasks;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface ICellManager
    {
        Task<Result> CreateCellEnvironment(string cellName, List<Column> columns);
        Task<Result> AddTables(string cellName, Table[] tables);
        Task<Result> DeleteTable(string cellName, string tableName);
        Task<Result<CellEnvironment>> GetEnvironment(string cellName);
        Task<Result<Column[]>> GetColumnsFromTable(string cellName, string tableName);
    }
}
