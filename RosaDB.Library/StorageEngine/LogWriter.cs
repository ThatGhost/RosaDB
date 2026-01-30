using System.IO.Abstractions;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.StorageEngine;

public class LogWriter(WriteAheadLogCache writeAheadLogCache, IFileSystem fileSystem) : ILogWriter
{
    public void Insert(string path, Row row, long logId)
    {
        if(!writeAheadLogCache.Logs.ContainsKey(path)) writeAheadLogCache.Logs.Add(path, []);
        Log log = new Log()
        {
            TupleData = row.BSON,
            IsDeleted = false,
            Id = logId,
        };
        writeAheadLogCache.Logs[path].Enqueue(log);
    }

    public void Update(string path, Row row, long logId) => Insert(path, row, logId);

    public void Delete(string path, long logId)
    {
        if(!writeAheadLogCache.Logs.ContainsKey(path)) writeAheadLogCache.Logs.Add(path, []);
        Log log = new Log()
        {
            IsDeleted = true,
            Id = logId,
        };
        writeAheadLogCache.Logs[path].Enqueue(log);
    }
    
    // TODO return dictionary with LogId, LogLocation
    public async ValueTask<Result> Commit()
    {
        foreach (var pathAndLog in writeAheadLogCache.Logs)
        {
            await using var fs = fileSystem.File.Open(pathAndLog.Key, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            await AssertLogFileExists(pathAndLog.Key, fs);

            try
            {
                fs.Seek(0, SeekOrigin.Begin);
                byte[] offsetBuffer = new byte[4];
                await fs.ReadExactlyAsync(offsetBuffer, 0, 4);
                int currentOffset = BitConverter.ToInt32(offsetBuffer, 0);
                
                fs.Seek(currentOffset, SeekOrigin.Begin);

                foreach (var log in pathAndLog.Value)
                {
                    byte[] isDeleted = BitConverter.GetBytes(log.IsDeleted);
                    byte[] id = BitConverter.GetBytes(log.Id);
                    byte[] versionFlag = BitConverter.GetBytes(1);
                    
                    int logLength = log.TupleData.Length + isDeleted.Length + id.Length + versionFlag.Length;
                    byte[] logLengthBytes = BitConverter.GetBytes(logLength);

                    await fs.WriteAsync(log.TupleData, 0, log.TupleData.Length);
                    await fs.WriteAsync(isDeleted, 0, isDeleted.Length);
                    await fs.WriteAsync(id, 0, id.Length);
                    await fs.WriteAsync(versionFlag, 0, versionFlag.Length);
                    await fs.WriteAsync(logLengthBytes, 0, logLengthBytes.Length);

                    currentOffset += logLength + 4;
                }
                
                fs.Seek(0, SeekOrigin.Begin);
                await fs.WriteAsync(BitConverter.GetBytes(currentOffset).AsMemory(0, 4));
            }
            catch (Exception ex)
            { 
                return new Error(ErrorPrefixes.DataError, $"Failed to commit data to {pathAndLog.Key}: {ex.Message}");
            }
        }
        
        // Clear cache after successful commit
        writeAheadLogCache.Logs.Clear();
        return Result.Success();
    }

    private readonly byte[] defaultHeader = [
        0x08, 0x00, 0x00, 0x00, // Offset (8)
        0x01, 0x00, 0x00, 0x00  // Version (1)
    ];
    private async ValueTask AssertLogFileExists(string path, FileSystemStream fs)
    {
        if (!fileSystem.File.Exists(path)) await fs.WriteAsync(defaultHeader);
    }

    public void Rollback()
    {
        writeAheadLogCache.Logs.Clear();
    }
}