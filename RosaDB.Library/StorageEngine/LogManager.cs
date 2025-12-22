using System.Security.Cryptography;
using System.Text;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.StorageEngine;

public class LogManager(LogCondenser logCondenser, SessionState sessionState)
{
    private const long MaxSegmentSize = 1024 * 1024; // 1MB
    private const int SparseIndexFrequency = 100;

    private readonly Dictionary<TableInstanceIdentifier, Queue<Log>> _writeAheadLogs = new();
    private readonly Dictionary<TableInstanceIdentifier, SegmentMetadata> _segmentMetadata = new();
    private readonly Dictionary<TableInstanceIdentifier, Dictionary<int, List<SparseIndexEntry>>> _sparseIndexCache = new();

    private Result<(SegmentMetadata metadata, string segmentFilePath, string indexFilePath, List<(Log log, byte[] bytes)> serializedLogs)> GetCommitFilePathsAndMetadata(TableInstanceIdentifier identifier, List<Log> condensedLogs)
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

        var indexFilePathResult = GetIndexFilePath(identifier, metadata.CurrentSegmentNumber);
        if (indexFilePathResult.IsFailure) return indexFilePathResult.Error;

        var segmentDirectory = Path.GetDirectoryName(segmentFilePathResult.Value);
        
        if (!string.IsNullOrEmpty(segmentDirectory) && !Directory.Exists(segmentDirectory))
        {
            Directory.CreateDirectory(segmentDirectory);
        }

        return (metadata, segmentFilePathResult.Value, indexFilePathResult.Value, serializedLogs);
    }

    private async Task WriteSegmentHeaderIfNeeded(long currentOffset, IndexHeader header, FileStream indexStream)
    {
        if (currentOffset == 0)
        {
            var headerBytes = IndexSerializer.Serialize(header);
            await indexStream.WriteAsync(headerBytes, CancellationToken.None);
        }
    }

    private async Task<long> WriteSerializedLogsAndSparseIndex(TableInstanceIdentifier identifier, SegmentMetadata metadata, List<(Log log, byte[] bytes)> serializedLogs, FileStream segmentStream, FileStream indexStream, long initialOffset)
    {
        long currentOffset = initialOffset;
        int recordCounter = 0;

        foreach ((Log log, byte[] bytes) in serializedLogs)
        {
            await segmentStream.WriteAsync(bytes, CancellationToken.None);
            
            if (recordCounter % SparseIndexFrequency == 0)
            {
                var indexEntry = new SparseIndexEntry(log.Id, currentOffset);
                var indexBytes = IndexSerializer.Serialize(indexEntry);
                await indexStream.WriteAsync(indexBytes, CancellationToken.None);

                if (!_sparseIndexCache.TryGetValue(identifier, out var segmentIndexes))
                {
                    segmentIndexes = new Dictionary<int, List<SparseIndexEntry>>();
                    _sparseIndexCache[identifier] = segmentIndexes;
                }
                if (!segmentIndexes.TryGetValue(metadata.CurrentSegmentNumber, out var sparseIndex))
                {
                    sparseIndex = new List<SparseIndexEntry>();
                    segmentIndexes[metadata.CurrentSegmentNumber] = sparseIndex;
                }
                sparseIndex.Add(indexEntry);
            }
            
            currentOffset += bytes.Length;
            recordCounter++;
        }
        return currentOffset;
    }

    public async Task LoadIndexesAsync()
    {
        if (sessionState.CurrentDatabase is null) return;

        _sparseIndexCache.Clear();
        _segmentMetadata.Clear();

        var dbPath = Path.Combine(FolderManager.BasePath, sessionState.CurrentDatabase.Name);
        if (!Directory.Exists(dbPath)) return;

        var indexFiles = Directory.GetFiles(dbPath, "*.idx", SearchOption.AllDirectories);

        foreach (var indexFile in indexFiles)
        {
            var indexBytes = await ByteReaderWriter.ReadBytesFromFile(indexFile, CancellationToken.None);
            if (indexBytes.Length == 0) continue;

            using var stream = new MemoryStream(indexBytes);
            
            var header = IndexSerializer.DeserializeHeader(stream);
            if (header == null) continue;
            
            var identifier = new TableInstanceIdentifier(header.Value.CellName, header.Value.TableName, header.Value.InstanceHash);
            var indexEntries = new List<SparseIndexEntry>();

            while (stream.Position < stream.Length)
            {
                var entry = IndexSerializer.DeserializeEntry(stream);
                if (entry == null) break;
                indexEntries.Add(entry.Value);
            }

            GetOrCreateSegmentIndexes(identifier)[header.Value.SegmentNumber] = indexEntries;

            var segmentFilePath = GetSegmentFilePath(identifier, header.Value.SegmentNumber);
            if (segmentFilePath.IsFailure) return;
            if (File.Exists(segmentFilePath.Value))
            {
                var segmentInfo = new FileInfo(segmentFilePath.Value);
                _segmentMetadata[identifier] = new SegmentMetadata(header.Value.SegmentNumber, segmentInfo.Length);
            }
        }
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

            
            Result<(SegmentMetadata metadata, string segmentFilePath, string indexFilePath, List<(Log log, byte[] bytes)> serializedLogs)> result = GetCommitFilePathsAndMetadata(identifier, condensedLogs);
            if (result.IsFailure) return result.Error;
            
            await using var segmentStream = new FileStream(result.Value.segmentFilePath, FileMode.Append, FileAccess.Write, FileShare.None);
            await using var indexStream = new FileStream(result.Value.indexFilePath, FileMode.Append, FileAccess.Write, FileShare.None);

            var header = new IndexHeader(identifier.CellName, identifier.TableName, identifier.InstanceHash, result.Value.metadata.CurrentSegmentNumber);
            await WriteSegmentHeaderIfNeeded(result.Value.metadata.CurrentSegmentSize, header, indexStream);

            var finalOffset = await WriteSerializedLogsAndSparseIndex(identifier, result.Value.metadata, result.Value.serializedLogs, segmentStream, indexStream, result.Value.metadata.CurrentSegmentSize);
            
            _segmentMetadata[identifier] = result.Value.metadata with { CurrentSegmentSize = finalOffset };
        }
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
        
        // Use first 2 chars of hash for bucketing to avoid large flat directories
        var hashPrefix = identifier.InstanceHash.Length >= 3 
            ? identifier.InstanceHash.Substring(0, 3) 
            : "xyz"; // Fallback for unexpected short hashes

        return Path.Combine(
            FolderManager.BasePath, sessionState.CurrentDatabase.Name, 
            identifier.CellName, identifier.TableName, 
            hashPrefix,
            $"{identifier.InstanceHash}_{segmentNumber}.dat");
    }

    private Result<string> GetIndexFilePath(TableInstanceIdentifier identifier, int segmentNumber)
    {
        var segmentPathResult = GetSegmentFilePath(identifier, segmentNumber);
        if (segmentPathResult.IsFailure) return segmentPathResult.Error;

        return Path.ChangeExtension(segmentPathResult.Value, ".idx");
    }

    private async Task<Result<Log>> FindOnDisk(TableInstanceIdentifier identifier, long logId)
    {
        if (!_sparseIndexCache.TryGetValue(identifier, out var segmentIndexes)) return new Error(ErrorPrefixes.FileError, "No index found for this table instance.");

        foreach (var segmentNumber in segmentIndexes.Keys.OrderByDescending(k => k))
        {
            var sparseIndex = segmentIndexes[segmentNumber];
            var indexEntryIndex = sparseIndex.FindLastIndex(e => e.Key <= logId);
            if (indexEntryIndex == -1) continue;

            var startEntry = sparseIndex[indexEntryIndex];
            long startOffset = startEntry.Offset;
            
            long endOffset = -1;
            if (indexEntryIndex + 1 < sparseIndex.Count) endOffset = sparseIndex[indexEntryIndex + 1].Offset;

            var segmentFilePath = GetSegmentFilePath(identifier, segmentNumber);
            if (segmentFilePath.IsFailure) return segmentFilePath.Error;
            int readLength = endOffset != -1 ? (int)(endOffset - startOffset) : -1;

            var bytesBlock = await ByteReaderWriter.ReadBytesFromFile(segmentFilePath.Value, startOffset, readLength, CancellationToken.None);
            if(bytesBlock.Length == 0) continue;

            using var stream = new MemoryStream(bytesBlock);
            while (stream.Position < stream.Length)
            {
                var log = await LogSerializer.DeserializeAsync(stream);
                if (log is null) break;
                if (log.Id == logId) return log;
            }
        }
        
        return new Error(ErrorPrefixes.FileError, "Log not found on disk.");
    }

    private Dictionary<int, List<SparseIndexEntry>> GetOrCreateSegmentIndexes(TableInstanceIdentifier identifier)
    {
        if (!_sparseIndexCache.TryGetValue(identifier, out var segmentIndexes))
        {
            segmentIndexes = new Dictionary<int, List<SparseIndexEntry>>();
            _sparseIndexCache[identifier] = segmentIndexes;
        }
        return segmentIndexes;
    }

    public async IAsyncEnumerable<Log> GetAllLogsForCellTable(string cellName, string tableName)
    {
        HashSet<long> seenLogIds = new HashSet<long>();

        var matchingIdentifiers = _sparseIndexCache.Keys
            .Where(id => id.CellName == cellName && id.TableName == tableName)
            .ToList();

        foreach (var identifier in matchingIdentifiers)
        {
            if (_sparseIndexCache.TryGetValue(identifier, out var segmentIndexes))
            {
                foreach (var segmentNumber in segmentIndexes.Keys.OrderByDescending(k => k)) // Order by segment number descending
                {
                    var segmentFilePath = GetSegmentFilePath(identifier, segmentNumber);
                    if (segmentFilePath.IsFailure || !File.Exists(segmentFilePath.Value)) continue;

                    var segmentLogs = new List<Log>();
                    await using var stream = new FileStream(segmentFilePath.Value, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
                    while (stream.Position < stream.Length)
                    {
                        var log = await LogSerializer.DeserializeAsync(stream);
                        if (log is null) break;
                        segmentLogs.Add(log);
                    }

                    segmentLogs.Reverse();

                    foreach (var log in segmentLogs)
                    {
                        if (seenLogIds.Add(log.Id) && !log.IsDeleted)
                        {
                            yield return log;
                        }
                    }
                }
            }
        }
    }

    public async IAsyncEnumerable<Log> GetAllLogsForCellInstanceTable(string cellName, string tableName, object[] indexValues)
    {
        HashSet<long> seenLogIds = new HashSet<long>();
        var identifier = CreateIdentifier(cellName, tableName, indexValues);

        // In memory logs - Reverse order to see newest first
        if (_writeAheadLogs.TryGetValue(identifier, out var inMemoryLogs))
        {
            foreach (var log in inMemoryLogs.Reverse())
            {
                if (seenLogIds.Add(log.Id))
                {
                    yield return log;
                }
            }
        }

        // Disk logs - Iterate segments in descending order (Newest segment first)
        if (_sparseIndexCache.TryGetValue(identifier, out var segmentIndexes))
        {
            foreach (var segmentNumber in segmentIndexes.Keys.OrderByDescending(k => k))
            {
                var segmentFilePath = GetSegmentFilePath(identifier, segmentNumber);
                if (segmentFilePath.IsFailure || !File.Exists(segmentFilePath.Value)) continue;

                var segmentLogs = new List<Log>();
                await using var stream = new FileStream(segmentFilePath.Value, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
                while (stream.Position < stream.Length)
                {
                    var log = await LogSerializer.DeserializeAsync(stream);
                    if (log is null) break;
                    segmentLogs.Add(log);
                }

                segmentLogs.Reverse();

                foreach (var log in segmentLogs)
                {
                    if (seenLogIds.Add(log.Id) && !log.IsDeleted)
                    {
                        yield return log;
                    }
                }
            }
        }
    }
}