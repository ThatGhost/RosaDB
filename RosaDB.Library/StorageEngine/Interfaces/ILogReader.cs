using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogReader
{
    Task<Result<Log>> FindLastestLog(string moduleName, string tableName, string instanceHash, long id);
    IAsyncEnumerable<Log> GetAllLogsForModuleTable(string moduleName, string tableName);
    IAsyncEnumerable<Log> GetAllLogsForModuleInstanceTable(string moduleName, string tableName, string instanceHash);
}