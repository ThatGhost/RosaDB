using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogReader
{
    Task<Result<Log>> FindLastestLog(string contextName, string tableName, string instanceHash, long id);
    IAsyncEnumerable<Log> GetAllLogsForContextTable(string contextName, string tableName);
    IAsyncEnumerable<Log> GetAllLogsForContextInstanceTable(string contextName, string tableName, string instanceHash);
}