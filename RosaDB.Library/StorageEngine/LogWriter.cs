using System.IO.Abstractions;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using RosaDB.Library.Websockets.Interfaces;

namespace RosaDB.Library.StorageEngine;

public class LogWriter(
    ILogCondenser logCondenser,
    SessionState sessionState,
    IFileSystem fileSystem,
    IFolderManager folderManager,
    IIndexManager indexManager,
    IContextManager contextManager,
    ISubscriptionManager subscriptionManager,
    WriteAheadLogCache writeAheadLogCache) : ILogWriter
{
    private const long MaxSegmentSize = 1024 * 1024;
    private readonly Dictionary<string, Stream> _activeStreams = new();

    private readonly Dictionary<TableInstanceIdentifier, SegmentMetadata> _segmentMetadata = new();

    public async ValueTask<Result> Commit()
    {
        if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database is not set");
        
        var logsToCommit = new Dictionary<TableInstanceIdentifier, Queue<Log>>(writeAheadLogCache.Logs);
        writeAheadLogCache.Logs.Clear();

        foreach ((TableInstanceIdentifier identifier, Queue<Log> logs) in logsToCommit)
        {
            var condensedLogs = logCondenser.Condense(logs).OrderBy(l => l.Id).ToList();
            if (condensedLogs.Count == 0) continue;

            Result<Column[]> columnsResult = await contextManager.GetColumnsFromTable(identifier.ContextName, identifier.TableName);
            if (!columnsResult.TryGetValue(out var columns)) return columnsResult.Error;

            Result<(SegmentMetadata metadata, string segmentFilePath, List<(Log log, byte[] bytes)> serializedLogs)> result = GetCommitFilePathsAndMetadata(identifier, condensedLogs);
            if (result.IsFailure) return result.Error;
            
            var path = result.Value.segmentFilePath;
            Stream segmentStream;
            if (_activeStreams.TryGetValue(path, out var stream)) segmentStream = stream;
            else
            {
                segmentStream = fileSystem.FileStream.New(path, FileMode.Append, FileAccess.Write, FileShare.Read);
                _activeStreams[path] = segmentStream;
            }

            var finalOffset = await WriteSerializedLogs(identifier, result.Value.metadata, result.Value.serializedLogs, segmentStream, result.Value.metadata.CurrentSegmentSize, columns);
            
            if (finalOffset == -1) return new Error(ErrorPrefixes.DataError, "Failed to write serialized logs and update indexes.");

            await segmentStream.FlushAsync();

            _segmentMetadata[identifier] = result.Value.metadata with { CurrentSegmentSize = finalOffset };
        }
        
        return Result.Success();
    }
    
    public void Rollback()
    {
        writeAheadLogCache.Logs.Clear();
    }
    
    public void Put(string contextName, string tableName, object[] tableIndex, byte[] data, List<(string Name, byte[] Value, bool IsPrimaryKey)>? indexValues = null, long? logId = null)
    {
        var identifier = InstanceHasher.CreateIdentifier(contextName, tableName, tableIndex);
        long finalLogId = logId ?? Guid.NewGuid().GetHashCode(); 
        
        Log log = new() { TupleData = data, Id = finalLogId, IndexValues = indexValues };
        PutLog(log, identifier);
    }

    public void Delete(string contextName, string tableName, object?[] indexValues, long logId)
    {
        var identifier = InstanceHasher.CreateIdentifier(contextName, tableName, indexValues);
        Log log = new() { Id = logId, IsDeleted = true };
        PutLog(log, identifier);
    }
    
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

    private async ValueTask<long> WriteSerializedLogs(TableInstanceIdentifier identifier, SegmentMetadata metadata, List<(Log log, byte[] bytes)> serializedLogs, Stream segmentStream, long initialOffset, Column[] columns)
    {
        long currentOffset = initialOffset;

        foreach ((Log log, byte[] bytes) in serializedLogs)
        {
            await segmentStream.WriteAsync(bytes, CancellationToken.None);
            indexManager.Insert(identifier, "LogId", IndexKeyConverter.ToByteArray(log.Id), new LogLocation(metadata.CurrentSegmentNumber, currentOffset));
            
            if (log.IsDeleted) continue;
            
            Result<Row> rowResult = RowSerializer.Deserialize(log.TupleData, columns);
            if (!rowResult.TryGetValue(out var row)) return -1;
            if (log.IndexValues != null)
            {
                foreach (var (name, val, isPk) in log.IndexValues)
                {
                    TableInstanceIdentifier indexIdentifier;
                    if (!isPk) indexIdentifier = identifier with { InstanceHash = "_TABLE_" };
                    else indexIdentifier = identifier;

                    indexManager.Insert(indexIdentifier, name, val, new LogLocation(metadata.CurrentSegmentNumber, currentOffset));
                }
            }
            else
            {
                for (int i = 0; i < row.Columns.Length; i++)
                {
                    Column col = row.Columns[i];
                    object? val = row.Values[i];

                    if (col.IsPrimaryKey || col.IsIndex)
                    {
                        byte[] keyBytes = IndexKeyConverter.ToByteArray(val);
                        TableInstanceIdentifier indexIdentifier;

                        if (col is { IsIndex: true, IsPrimaryKey: false }) indexIdentifier = identifier with { InstanceHash = "_TABLE_" };
                        else indexIdentifier = identifier;

                        indexManager.Insert(indexIdentifier, col.Name, keyBytes, new LogLocation(metadata.CurrentSegmentNumber, currentOffset));
                    }
                }
            }
            
            currentOffset += bytes.Length;
            _ = subscriptionManager.NotifySubscriber(identifier, row);
        }
        return currentOffset;
    }

    private void PutLog(Log log, TableInstanceIdentifier identifier)
    {
        if (!writeAheadLogCache.Logs.TryGetValue(identifier, out var logs))
        {
            logs = new Queue<Log>();
            writeAheadLogCache.Logs[identifier] = logs;
        }
        logs.Enqueue(log);
    }

    private Result<string> GetSegmentFilePath(TableInstanceIdentifier identifier, int segmentNumber)
    {
        if (sessionState.CurrentDatabase is null) return new Error(ErrorPrefixes.StateError, "Database is not set");
        
        var hashPrefix = identifier.InstanceHash.Length >= 2 
            ? identifier.InstanceHash.Substring(0, 2) 
            : "xy"; 

        return fileSystem.Path.Combine(
            folderManager.BasePath, sessionState.CurrentDatabase.Name, 
            identifier.ContextName, identifier.TableName, 
            hashPrefix,
            $"{identifier.InstanceHash}_{segmentNumber}.dat");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var stream in _activeStreams.Values)
        {
            await stream.DisposeAsync();
        }
        _activeStreams.Clear();
    }
}