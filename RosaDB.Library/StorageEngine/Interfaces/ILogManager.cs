using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogManager : IAsyncDisposable
{
    ValueTask<Result> Commit();
    void Rollback();
    void Put(string contextName, string tableName, object[] tableIndex, byte[] data, List<(string Name, byte[] Value, bool IsPrimaryKey)>? indexValues = null, long? logId = null);
    void Delete(string contextName, string tableName, object?[] indexValues, long logId);
    Task<Result<Log>> FindLastestLog(string contextName, string tableName, object[] indexValues, long id);
    IAsyncEnumerable<Log> GetAllLogsForContextTable(string contextName, string tableName);
    IAsyncEnumerable<Log> GetAllLogsForContextInstanceTable(string contextName, string tableName, object?[] indexValues);
}