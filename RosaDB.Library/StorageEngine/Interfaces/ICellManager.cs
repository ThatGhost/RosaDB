using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using System.Threading.Tasks;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface ICellManager
    {
        Task<Result> CreateCellEnvironment(string cellName, Column[] columns);
        Task<Result> CreateTable(string cellName, Table table);
        Task<Result> DeleteTable(string cellName, string tableName);
        Task<Result<CellEnvironment>> GetEnvironment(string cellName);
        Task<Result<Column[]>> GetColumnsFromTable(string cellName, string tableName);
        Task<Result> CreateCellInstance(string cellGroupName, string instanceHash, Row instanceData, Column[] schema);
        Task<Result<Row>> GetCellInstance(string cellGroupName, string instanceHash);
    }
}
