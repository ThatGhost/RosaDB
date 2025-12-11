using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine;

public class LogManager
{
    private readonly Dictionary<Cell, Dictionary<Table, Queue<Log>>> _writeAheadLogs = new();
    private readonly Dictionary<Cell, Dictionary<Table, long>> _latestIndex = new();
    private readonly LogCondenser _logCondenser = new();
    private const int MaxLogQueueSize = 20;
    
    public void Put(Cell cell, Table table, long? logId, byte[] data)
    {
        long finalLogId = logId ?? GetLatestLogId(cell, table);
        Log log = new Log()
        {
            TupleData = data,
            Id = finalLogId,
        };
        PutLog(log, cell, table);
    }

    public void Delete(Cell cell, Table table, long logId)
    {
        Log log = new Log()
        {
            Id = logId,
            IsDeleted = true,
        };
        PutLog(log, cell, table);
    }
    
    public Result<Log> FindLastestLog(Cell cell, Table table, long id)
    {
        if (!_writeAheadLogs.TryGetValue(cell, out var cellLogs)) return new Error(ErrorPrefixes.FileError, string.Empty);
        if (!cellLogs.TryGetValue(table, out var logs)) return new Error(ErrorPrefixes.FileError, string.Empty);
        
        Log? log = logs.FirstOrDefault(l => l.Id == id);
        return log is null ? new Error(ErrorPrefixes.FileError, string.Empty) : log;
    }
    
    private void PutLog(Log log, Cell cell, Table table)
    {
        Dictionary<Table, Queue<Log>> tableLogs = FindOrCreateTableLogs(cell);
        Queue<Log> logs = FindOrCreateLogs(tableLogs, table);
        logs.Enqueue(log);

        if (logs.Count >= MaxLogQueueSize)
        {
            var condensedLogs = _logCondenser.Condense(logs);
            // TODO: Persist the condensed logs
            logs.Clear();
        }
    }
    
    private Dictionary<Table, Queue<Log>> FindOrCreateTableLogs(Cell cell)
    {
        if (_writeAheadLogs.TryGetValue(cell, out var list)) return list;
        return _writeAheadLogs[cell] = [];
    }
    private Queue<Log> FindOrCreateLogs(Dictionary<Table, Queue<Log>> tableLogs, Table table)
    {
        if (tableLogs.TryGetValue(table, out var logs)) return logs;
        return tableLogs[table] = [];
    }
    
    private Dictionary<Table, long> FindOrCreateTableIndexes(Cell cell)
    {
        if (_latestIndex.TryGetValue(cell, out var list)) return list;
        return _latestIndex[cell] = [];
    }
    
    private long FindOrCreateIndex(Dictionary<Table, long> tableIndexes, Table table)
    {
        if (tableIndexes.TryGetValue(table, out var index)) return index;
        return tableIndexes[table] = 0;
    }

    public long GetLatestLogId(Cell cell, Table table)
    {
        Dictionary<Table, long> tableIndexes = FindOrCreateTableIndexes(cell);
        return FindOrCreateIndex(tableIndexes, table);
    }
}