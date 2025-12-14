using System.Security.Cryptography;
using System.Text;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;

namespace RosaDB.Library.StorageEngine;

public class LogManager(LogCondenser logCondenser, SessionState sessionState)
{
    private readonly record struct TableInstanceIdentifier(string CellName, string TableName, string InstanceHash);

    private readonly Dictionary<TableInstanceIdentifier, Queue<Log>> _writeAheadLogs = new();
    private readonly Dictionary<TableInstanceIdentifier, long> _latestIndex = new();

    public async Task Commit()
    {
        if (sessionState.CurrentDatabase is null)
        {
            return;
        }

        foreach (var (identifier, logs) in _writeAheadLogs)
        {
            var condensedLogs = logCondenser.Condense(logs);
            if (condensedLogs.Count == 0)
            {
                continue;
            }

            var logFilePath = GetLogFilePath(identifier);
            var logDirectory = Path.GetDirectoryName(logFilePath);
            if (!string.IsNullOrEmpty(logDirectory) && !Directory.Exists(logDirectory))
            {
                Directory.CreateDirectory(logDirectory);
            }

            // Current simplification. Will need to make an index file and save a btree in the future. also pages...
            foreach (var log in condensedLogs)
            {
                var bytes = ByteObjectConverter.ObjectToByteArray(log);
                await ByteReaderWriter.AppendBytesToFile(logFilePath, bytes, CancellationToken.None);
            }
        }
        
        _writeAheadLogs.Clear();
    }
    
    public void Put(Cell cell, Table table, object[] indexValues, long? logId, byte[] data)
    {
        var identifier = CreateIdentifier(cell, table, indexValues);
        long finalLogId = logId ?? GetLatestLogId(identifier);
        Log log = new Log()
        {
            TupleData = data,
            Id = finalLogId,
        };
        PutLog(log, identifier);
    }

    public void Delete(Cell cell, Table table, object[] indexValues, long logId)
    {
        var identifier = CreateIdentifier(cell, table, indexValues);
        Log log = new Log()
        {
            Id = logId,
            IsDeleted = true,
        };
        PutLog(log, identifier);
    }
    
    public Result<Log> FindLastestLog(Cell cell, Table table, object[] indexValues, long id)
    {
        var identifier = CreateIdentifier(cell, table, indexValues);
        if (!_writeAheadLogs.TryGetValue(identifier, out var logs))
        {
            return new Error(ErrorPrefixes.FileError, "Log not found in memory.");
        }
        
        Log? log = logs.AsEnumerable().Reverse().FirstOrDefault(l => l.Id == id);
        return log is null ? new Error(ErrorPrefixes.FileError, "Log not found in memory.") : log;
    }
    
    private void PutLog(Log log, TableInstanceIdentifier identifier)
    {
        if (!_writeAheadLogs.TryGetValue(identifier, out var logs))
        {
            logs = new Queue<Log>();
            _writeAheadLogs[identifier] = logs;
        }
        logs.Enqueue(log);
    }
    
    private long GetLatestLogId(TableInstanceIdentifier identifier)
    {
        var index = _latestIndex.GetValueOrDefault(identifier, 0);
        _latestIndex[identifier] = index + 1;
        return index + 1;
    }

    private TableInstanceIdentifier CreateIdentifier(Cell cell, Table table, object[] indexValues)
    {
        var indexString = string.Join(";", indexValues.Select(v => v.ToString()));
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(indexString)));
        return new TableInstanceIdentifier(cell.Name, table.Name, hash);
    }

    private string GetLogFilePath(TableInstanceIdentifier identifier)
    {
        if (sessionState.CurrentDatabase is null)
        {
            throw new InvalidOperationException("Current database is not set.");
        }
        
        return Path.Combine(
            FolderManager.BasePath, 
            sessionState.CurrentDatabase.Name, 
            identifier.CellName, 
            identifier.TableName, 
            $"{identifier.InstanceHash}.log");
    }
}
