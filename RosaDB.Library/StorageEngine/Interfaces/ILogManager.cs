using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogManager : IAsyncDisposable
{
    ValueTask<Result> Commit();
    void Put(string cellName, string tableName, object[] tableIndex, byte[] data, List<(string Name, byte[] Value, bool IsPrimaryKey)>? indexValues = null, long? logId = null);
    void Delete(string cellName, string tableName, object[] indexValues, long logId);
    Task<Result<Log>> FindLastestLog(string cellName, string tableName, object[] indexValues, long id);
    Task<Result<Log>> GetLogAtLocation(LogLocation logLocation);
    IAsyncEnumerable<Log> GetAllLogsForCellTable(string cellName, string tableName);
    IAsyncEnumerable<Log> GetAllLogsForCellInstanceTable(string cellName, string tableName, object?[] indexValues);
}