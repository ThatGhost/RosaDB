using System.IO.Abstractions;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.StorageEngine;

public class LogWriter(WriteAheadLogCache writeAheadLogCache, IFileSystem fileSystem) : ILogWriter
{
    public void Insert(string path, Row row)
    {
        throw new NotImplementedException();
    }

    public void Update(string path, Row row, long logId)
    {
        throw new NotImplementedException();
    }

    public void Delete(string path, long logId)
    {
        throw new NotImplementedException();
    }
    
    public async ValueTask<Result> Commit()
    {
        return Result.Success();
    }

    public void Rollback()
    {
        writeAheadLogCache.Logs.Clear();
    }
}