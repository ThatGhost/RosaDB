using System.IO.Abstractions;
using System.Security.Cryptography;
using System.Text;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.StorageEngine;

public class LogManager(
    LogCondenser logCondenser,
    SessionState sessionState,
    IFileSystem fileSystem,
    IFolderManager folderManager,
    IIndexManager indexManager,
    ICellManager cellManager)
{
    private const long MaxSegmentSize = 1024 * 1024;

    private readonly Dictionary<TableInstanceIdentifier, Queue<Log>> _writeAheadLogs = new();
    private readonly Dictionary<TableInstanceIdentifier, SegmentMetadata> _segmentMetadata = new();

    private Result<(SegmentMetadata metadata, string segmentFilePath, List<(Log log, byte[] bytes)> serializedLogs)> GetCommitFilePathsAndMetadata(TableInstanceIdentifier identifier, List<Log> condensedLogs)
    {
        if (!_segmentMetadata.TryGetValue(identifier, out SegmentMetadata metadata)) metadata = new SegmentMetadata(0, 0); 
        
        long batchSize = 0;
        var serializedLogs = new List<(Log log, byte[] bytes)>();
        foreach (var log in condensedLogs)
        {
            var bytes = LogSerializer.Serialize(log);
            serializedLogs.Add((log, bytes));
            batchSize += bytes.Length;
        }
        
        if (metadata.CurrentSegmentSize + batchSize > MaxSegmentSize)
        {
            metadata = new SegmentMetadata(metadata.CurrentSegmentNumber + 1, 0);
        }

        var segmentFilePathResult = GetSegmentFilePath(identifier, metadata.CurrentSegmentNumber);
        if (segmentFilePathResult.IsFailure) return segmentFilePathResult.Error;
        
        var segmentDirectory = fileSystem.Path.GetDirectoryName(segmentFilePathResult.Value);
        
        if (!string.IsNullOrEmpty(segmentDirectory) && !fileSystem.Directory.Exists(segmentDirectory))
        {
            fileSystem.Directory.CreateDirectory(segmentDirectory);
        }

        return (metadata, segmentFilePathResult.Value, serializedLogs);
    }

    private async Task<long> WriteSerializedLogs(TableInstanceIdentifier identifier, SegmentMetadata metadata, List<(Log log, byte[] bytes)> serializedLogs, Stream segmentStream, long initialOffset, Column[] columns)
    {
        long currentOffset = initialOffset;

        foreach ((Log log, byte[] bytes) in serializedLogs)
        {
            await segmentStream.WriteAsync(bytes, CancellationToken.None);
            
            indexManager.Insert(identifier, "LogId", IndexKeyConverter.ToByteArray(log.Id), new LogLocation(metadata.CurrentSegmentNumber, currentOffset));
            
            Result<Row> rowResult = RowSerializer.Deserialize(log.TupleData, columns);
            if (rowResult.IsFailure) return -1;

            Row row = rowResult.Value;
            for (int i = 0; i < row.Columns.Length; i++)
            {
                Column col = row.Columns[i];
                object? val = row.Values[i];

                if (col.IsPrimaryKey || col.IsIndex)
                {
                    byte[] keyBytes = IndexKeyConverter.ToByteArray(val);
                    TableInstanceIdentifier indexIdentifier;

                    // Use a table-wide identifier for secondary indexes, and instance-specific for primary keys.
                    if (col.IsIndex && !col.IsPrimaryKey)
                    {
                        indexIdentifier = new TableInstanceIdentifier(identifier.CellName, identifier.TableName, "_TABLE_");
                    }
                    else // This is a primary key
                    {
                        indexIdentifier = identifier;
                    }
                    
                    indexManager.Insert(indexIdentifier, col.Name, keyBytes, new LogLocation(metadata.CurrentSegmentNumber, currentOffset));
                }
            }
            
            currentOffset += bytes.Length;
        }
        return currentOffset;
    }

    public async Task<Result> Commit()
    {
        if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database is not set");
        
        var logsToCommit = new Dictionary<TableInstanceIdentifier, Queue<Log>>(_writeAheadLogs);
        _writeAheadLogs.Clear();

        foreach ((TableInstanceIdentifier identifier, Queue<Log> logs) in logsToCommit)
        {
            var condensedLogs = logCondenser.Condense(logs).OrderBy(l => l.Id).ToList();
            if (condensedLogs.Count == 0) continue;

            Result<Column[]> columnsResult = await cellManager.GetColumnsFromTable(identifier.CellName, identifier.TableName);
            if (!columnsResult.TryGetValue(out var columns)) return columnsResult.Error;

            Result<(SegmentMetadata metadata, string segmentFilePath, List<(Log log, byte[] bytes)> serializedLogs)> result = GetCommitFilePathsAndMetadata(identifier, condensedLogs);
            if (result.IsFailure) return result.Error;
            
            await using var segmentStream = fileSystem.FileStream.New(result.Value.segmentFilePath, FileMode.Append, FileAccess.Write, FileShare.None);

            var finalOffset = await WriteSerializedLogs(identifier, result.Value.metadata, result.Value.serializedLogs, segmentStream, result.Value.metadata.CurrentSegmentSize, columns);
            
            if (finalOffset == -1) return new Error(ErrorPrefixes.DataError, "Failed to write serialized logs and update indexes.");

            _segmentMetadata[identifier] = result.Value.metadata with { CurrentSegmentSize = finalOffset };
        }
        
        indexManager.CloseAllIndexes();
        return Result.Success();
    }
    
    public void Put(string cellName, string tableName, object[] tableIndex, byte[] data, long? logId = null)
    {
        var identifier = CreateIdentifier(cellName, tableName, tableIndex);
        long finalLogId = logId ?? Guid.NewGuid().GetHashCode(); 
        
        Log log = new() { TupleData = data, Id = finalLogId };
        PutLog(log, identifier);
    }

    public void Delete(string cellName, string tableName, object[] indexValues, long logId)
    {
        var identifier = CreateIdentifier(cellName, tableName, indexValues);
        Log log = new() { Id = logId, IsDeleted = true };
        PutLog(log, identifier);
    }
    
    public async Task<Result<Log>> FindLastestLog(string cellName, string tableName, object[] indexValues, long id)
    {
        var identifier = CreateIdentifier(cellName, tableName, indexValues);
        
        if (_writeAheadLogs.TryGetValue(identifier, out var logs))
        {
            Log? log = logs.AsEnumerable().Reverse().FirstOrDefault(l => l.Id == id);
            if (log is not null) return log;
        }
        
        return await FindOnDisk(identifier, id);
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
    
    private TableInstanceIdentifier CreateIdentifier(string cellName, string tableName, object[] indexValues)
    {
        var indexString = string.Join(";", indexValues.Select(v => v.ToString()));
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(indexString)));
        return new TableInstanceIdentifier(cellName, tableName, hash);
    }

    private Result<string> GetSegmentFilePath(TableInstanceIdentifier identifier, int segmentNumber)
    {
        if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database is not set");
        
        var hashPrefix = identifier.InstanceHash.Length >= 2 
            ? identifier.InstanceHash.Substring(0, 2) 
            : "xy"; 

        return fileSystem.Path.Combine(
            folderManager.BasePath, sessionState.CurrentDatabase.Name, 
            identifier.CellName, identifier.TableName, 
            hashPrefix,
            $"{identifier.InstanceHash}_{segmentNumber}.dat");
    }

    private async Task<Result<Log>> FindOnDisk(TableInstanceIdentifier identifier, long logId)
    {
        var logLocationResult = indexManager.Search(identifier, "LogId", IndexKeyConverter.ToByteArray(logId));
        if (logLocationResult.IsFailure) return logLocationResult.Error;

        var logLocation = logLocationResult.Value;
        var segmentFilePathResult = GetSegmentFilePath(identifier, logLocation.SegmentNumber);
        if (!segmentFilePathResult.TryGetValue(out var segmentFilePath)) return segmentFilePathResult.Error;

        if (!fileSystem.File.Exists(segmentFilePath))
            return new Error(ErrorPrefixes.FileError, $"Segment file not found for log location: {logLocation.SegmentNumber}.");

        await using var stream = fileSystem.FileStream.New(segmentFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        stream.Seek(logLocation.Offset, SeekOrigin.Begin);
        
        var log = await LogSerializer.DeserializeAsync(stream);
        if (log is null) return new Error(ErrorPrefixes.FileError, "Failed to deserialize log from disk.");
        
        return log;
    }

    // needs refactoring to where LogLocation gives a hash to the segment file
    public async Task<Result<Log>> GetLogAtLocation(LogLocation logLocation)
    {
        if (sessionState.CurrentDatabase is null)
        {
            return new Error(ErrorPrefixes.StateError, "Database not set");
        }

        var dbPath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name);
        var allSegmentFiles = fileSystem.Directory.GetFiles(dbPath, "*.dat", SearchOption.AllDirectories);

        string segmentFileName = $"_{logLocation.SegmentNumber}.dat";
        var segmentFilePath = allSegmentFiles.FirstOrDefault(f => f.EndsWith(segmentFileName));

        if (segmentFilePath == null || !fileSystem.File.Exists(segmentFilePath))
            return new Error(ErrorPrefixes.FileError, $"Segment file not found for log location: {logLocation.SegmentNumber}.");

        await using var stream = fileSystem.FileStream.New(segmentFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        stream.Seek(logLocation.Offset, SeekOrigin.Begin);
        
        var log = await LogSerializer.DeserializeAsync(stream);
        if (log is null) return new Error(ErrorPrefixes.FileError, "Failed to deserialize log from disk.");
        
        return log;
    }

    public async IAsyncEnumerable<Log> GetAllLogsForCellTable(string cellName, string tableName)
    {
        if (sessionState.CurrentDatabase is null) yield break;

        var tablePath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, cellName, tableName);
        if (!fileSystem.Directory.Exists(tablePath)) yield break;

        var dataFiles = fileSystem.Directory.GetFiles(tablePath, "*.dat", SearchOption.AllDirectories);

        var seenLogIds = new HashSet<long>();
        var allLogs = new List<Log>();

        foreach (var dataFile in dataFiles)
        {
            await using var stream = fileSystem.FileStream.New(dataFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            while (stream.Position < stream.Length)
            {
                var log = await LogSerializer.DeserializeAsync(stream);
                if (log is null) break;
                allLogs.Add(log);
            }
        }

        foreach (var log in allLogs.OrderByDescending(l => l.Date))
        {
            if (seenLogIds.Add(log.Id) && !log.IsDeleted)
            {
                yield return log;
            }
        }
    }

    public async IAsyncEnumerable<Log> GetAllLogsForCellInstanceTable(string cellName, string tableName, object[] indexValues)
    {
        if (sessionState.CurrentDatabase is null) throw new Exception();
        var identifier = CreateIdentifier(cellName, tableName, indexValues);

        var allLogs = new List<Log>();

        if (_writeAheadLogs.TryGetValue(identifier, out var inMemoryLogs))
        {
            allLogs.AddRange(inMemoryLogs);
        }
        
        var dbPath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name);
        var instancePath = fileSystem.Path.Combine(dbPath, identifier.CellName, identifier.TableName, identifier.InstanceHash.Substring(0, 2));

        if (fileSystem.Directory.Exists(instancePath))
        {
            var dataFiles = fileSystem.Directory.GetFiles(instancePath, $"{identifier.InstanceHash}_*.dat");

            foreach (var dataFile in dataFiles)
            {
                await using var stream = fileSystem.FileStream.New(dataFile, FileMode.Open, FileAccess.Read, FileShare.Read);
                while (stream.Position < stream.Length)
                {
                    var log = await LogSerializer.DeserializeAsync(stream);
                    if (log is null) break;
                    allLogs.Add(log);
                }
            }
        }
        
        var seenLogIds = new HashSet<long>();
        foreach (var log in allLogs.OrderByDescending(l => l.Date))
        {
            if (seenLogIds.Add(log.Id) && !log.IsDeleted)
            {
                yield return log;
            }
        }
    }
}