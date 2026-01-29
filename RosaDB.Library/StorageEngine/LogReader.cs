using System.IO.Abstractions;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.StorageEngine;

public class LogReader(IFileSystem fileSystem, WriteAheadLogCache writeAheadLogCache) : ILogReader
{
    public Task<Result<Log>> FindLastestLog(string path, long id)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<Log> GetAllLogs(string path)
    {
        throw new NotImplementedException();
    }
}