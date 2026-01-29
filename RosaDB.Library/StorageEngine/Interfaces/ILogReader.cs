using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogReader
{
    Task<Result<Log>> FindLastestLog(string path, long id);
    IAsyncEnumerable<Log> GetAllLogs(string path);
}