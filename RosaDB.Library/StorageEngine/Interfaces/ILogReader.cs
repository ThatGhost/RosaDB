using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogReader
{
    public Task<Result<Log?>> FindLog(string path, long id);
    public Task<Result<Log>> FindLog(LogLocation location);
    public IAsyncEnumerable<Log> GetAllLogs(string path);
}