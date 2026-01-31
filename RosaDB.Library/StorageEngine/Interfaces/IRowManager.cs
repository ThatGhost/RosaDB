using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IRowManager
{
    public Task<Result> InsertRow(Row row, string moduleName, string tableName, string instanceHash);
    public Task<Result> UpdateRow(Row row, string moduleName, string tableName, string instanceHash, long logId);
    public Task<Result> DeleteRow(string moduleName, string tableName, string instanceHash, long logId);
    public Task<Result> Commit();
    public Task<Result> Rollback();
}