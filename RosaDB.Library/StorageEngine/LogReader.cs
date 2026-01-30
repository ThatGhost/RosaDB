using System.IO.Abstractions;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.StorageEngine;

public class LogReader(IFileSystem fileSystem, WriteAheadLogCache writeAheadLogCache) : ILogReader
{
    public async Task<Result<Log?>> FindLastestLog(string path, long id)
    {
        writeAheadLogCache.Logs.TryGetValue(path, out var logs);
        if (logs is not null)
        {
            var log = logs.FirstOrDefault(l => l.Id == id);
            if (log is not null) return log;
        }

        await foreach (var log in GetAllLogs(path))
        {
            if (log.Id == id && !log.IsDeleted) return log;
        }

        return Result<Log?>.Success(null);
    }

    public async IAsyncEnumerable<Log> GetAllLogs(string path)
    {
        HashSet<long> seenLogIds = [];
        
        const int headerSize = 8;
        if (!fileSystem.File.Exists(path)) yield break;

        await using var fs = fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (fs.Length <= 8) yield break;

        byte[] offsetBuffer = new byte[4];
        await fs.ReadExactlyAsync(offsetBuffer, 0, 4);
        int endOfLogsOffset = BitConverter.ToInt32(offsetBuffer, 0);
        long currentPosition = endOfLogsOffset;

        while (currentPosition > headerSize)
        {
            var (log, nextLogStartPosition) = await ReadLog(fs, currentPosition);
            
            if(seenLogIds.Contains(log.Id)) continue;
            if (log.IsDeleted) { seenLogIds.Add(log.Id); continue; }
            
            yield return log;
            
            seenLogIds.Add(log.Id);
            currentPosition = nextLogStartPosition;
        }
    }

    private async ValueTask<(Log log, long logContentStartPosition)> ReadLog(FileSystemStream fs, long currentPosition)
    {
        fs.Seek(currentPosition - 4, SeekOrigin.Begin);
        byte[] logLengthBuffer = new byte[4];
        await fs.ReadExactlyAsync(logLengthBuffer, 0, 4);
        int logLength = BitConverter.ToInt32(logLengthBuffer, 0);

        long logContentStartPosition = currentPosition - 4 - logLength;
        fs.Seek(logContentStartPosition, SeekOrigin.Begin);

        byte[] logContentBuffer = new byte[logLength];
        await fs.ReadExactlyAsync(logContentBuffer, 0, logLength);
            
        // Deserialize the log content buffer.
        // Structure: [TupleData][IsDeleted(1 byte)][Id(8 bytes)][Version(4 bytes)]
        int tupleDataLength = logLength - 1 - 8 - 4;
        byte[] tupleData = new byte[tupleDataLength];
        Array.Copy(logContentBuffer, 0, tupleData, 0, tupleDataLength);

        bool isDeleted = BitConverter.ToBoolean(logContentBuffer, tupleDataLength);
        long id = BitConverter.ToInt64(logContentBuffer, tupleDataLength + 1);

        return (new Log
        {
            Id = id,
            IsDeleted = isDeleted,
            TupleData = tupleData,
        }, logContentStartPosition);
    }
}