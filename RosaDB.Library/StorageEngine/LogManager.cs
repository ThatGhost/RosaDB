using System.Security.Cryptography;
using System.Text;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;

namespace RosaDB.Library.StorageEngine;

public class LogManager(LogCondenser logCondenser, SessionState sessionState)
{
    private const long MaxSegmentSize = 1024 * 1024; // 1MB
    private const int SparseIndexFrequency = 100;

    private readonly record struct TableInstanceIdentifier(string CellName, string TableName, string InstanceHash);
    private readonly record struct SegmentMetadata(int CurrentSegmentNumber, long CurrentSegmentSize);
    private readonly record struct SparseIndexEntry(long Key, long Offset);
    private readonly record struct IndexHeader(string CellName, string TableName, string InstanceHash, int SegmentNumber);

    private readonly Dictionary<TableInstanceIdentifier, Queue<Log>> _writeAheadLogs = new();
    private readonly Dictionary<TableInstanceIdentifier, SegmentMetadata> _segmentMetadata = new();
    private readonly Dictionary<TableInstanceIdentifier, Dictionary<int, List<SparseIndexEntry>>> _sparseIndexCache = new();

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
            
            var header = ByteObjectConverter.ReadObjectFromStream<IndexHeader>(stream);
            if (header.Equals(default(IndexHeader))) continue;
            
            var identifier = new TableInstanceIdentifier(header.CellName, header.TableName, header.InstanceHash);
            var indexEntries = new List<SparseIndexEntry>();

            while (stream.Position < stream.Length)
            {
                var entry = ByteObjectConverter.ReadObjectFromStream<SparseIndexEntry>(stream);
                if (entry.Equals(default(SparseIndexEntry))) continue;
                indexEntries.Add(entry);
            }

            GetOrCreateSegmentIndexes(identifier)[header.SegmentNumber] = indexEntries;

            var segmentFilePath = GetSegmentFilePath(identifier, header.SegmentNumber);
            if (File.Exists(segmentFilePath))
            {
                var segmentInfo = new FileInfo(segmentFilePath);
                _segmentMetadata[identifier] = new SegmentMetadata(header.SegmentNumber, segmentInfo.Length);
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

            if (!_segmentMetadata.TryGetValue(identifier, out SegmentMetadata metadata)) metadata = new SegmentMetadata(0, 0); 
            
            long batchSize = 0;
            var serializedLogs = new List<(Log log, byte[] bytes)>();
            foreach (var log in condensedLogs)
            {
                var bytes = ByteObjectConverter.ObjectToByteArray(log);
                serializedLogs.Add((log, bytes));
                batchSize += bytes.Length;
            }
            
            if (metadata.CurrentSegmentSize + batchSize > MaxSegmentSize)
            {
                metadata = new SegmentMetadata(metadata.CurrentSegmentNumber + 1, 0);
            }

            var segmentFilePath = GetSegmentFilePath(identifier, metadata.CurrentSegmentNumber);
            var indexFilePath = GetIndexFilePath(identifier, metadata.CurrentSegmentNumber);
            var segmentDirectory = Path.GetDirectoryName(segmentFilePath);
            
            if (!string.IsNullOrEmpty(segmentDirectory) && !Directory.Exists(segmentDirectory))
            {
                Directory.CreateDirectory(segmentDirectory);
            }
            
            long currentOffset = metadata.CurrentSegmentSize;
            if (!File.Exists(segmentFilePath))
            {
                currentOffset = 0;
                var header = new IndexHeader(identifier.CellName, identifier.TableName, identifier.InstanceHash, metadata.CurrentSegmentNumber);
                var headerBytes = ByteObjectConverter.ObjectToByteArray(header);
                await ByteReaderWriter.AppendBytesToFile(indexFilePath, headerBytes, CancellationToken.None);
            }

            int recordCounter = 0;
            foreach ((Log log, byte[] bytes) in serializedLogs)
            {
                await ByteReaderWriter.AppendBytesToFile(segmentFilePath, bytes, CancellationToken.None);
                
                if (recordCounter % SparseIndexFrequency == 0)
                {
                    var indexEntry = new SparseIndexEntry(log.Id, currentOffset);
                    var indexBytes = ByteObjectConverter.ObjectToByteArray(indexEntry); 
                    await ByteReaderWriter.AppendBytesToFile(indexFilePath, indexBytes, CancellationToken.None);

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
            
            _segmentMetadata[identifier] = metadata with { CurrentSegmentSize = currentOffset };
        }
        return Result.Success();
    }
    
    public void Put(string cellName, string tableName, object[] indexValues, byte[] data, long? logId = null)
    {
        var identifier = CreateIdentifier(cellName, tableName, indexValues);
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

    private string GetSegmentFilePath(TableInstanceIdentifier identifier, int segmentNumber)
    {
        if (sessionState.CurrentDatabase is null) throw new InvalidOperationException("Current database is not set.");
        
        return Path.Combine(
            FolderManager.BasePath, sessionState.CurrentDatabase.Name, 
            identifier.CellName, identifier.TableName, 
            $"{identifier.InstanceHash}_{segmentNumber}.dat");
    }

    private string GetIndexFilePath(TableInstanceIdentifier identifier, int segmentNumber)
    {
        var segmentPath = GetSegmentFilePath(identifier, segmentNumber);
        return Path.ChangeExtension(segmentPath, ".idx");
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
            int readLength = endOffset != -1 ? (int)(endOffset - startOffset) : -1;

            var bytesBlock = await ByteReaderWriter.ReadBytesFromFile(segmentFilePath, startOffset, readLength, CancellationToken.None);
            if(bytesBlock.Length == 0) continue;

            using var stream = new MemoryStream(bytesBlock);
            while (stream.Position < stream.Length)
            {
                var log = ByteObjectConverter.ReadObjectFromStream<Log>(stream);
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
                foreach (var segmentNumber in segmentIndexes.Keys.OrderBy(k => k)) // Order by segment number
                {
                    var segmentFilePath = GetSegmentFilePath(identifier, segmentNumber);
                    if (!File.Exists(segmentFilePath)) continue;

                    await using var stream = new FileStream(segmentFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
                    while (stream.Position < stream.Length)
                    {
                        var log = await ByteObjectConverter.ReadObjectFromStreamAsync<Log>(stream);
                        if (log is null) break;
                        
                        if (seenLogIds.Add(log.Id))
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

        // In memory logs
        if (_writeAheadLogs.TryGetValue(identifier, out var inMemoryLogs))
        {
            foreach (var log in inMemoryLogs)
            {
                if (seenLogIds.Add(log.Id))
                {
                    yield return log;
                }
            }
        }

        // Disk logs
        if (_sparseIndexCache.TryGetValue(identifier, out var segmentIndexes))
        {
            foreach (var segmentNumber in segmentIndexes.Keys.OrderBy(k => k))
            {
                var segmentFilePath = GetSegmentFilePath(identifier, segmentNumber);
                if (!File.Exists(segmentFilePath)) continue;

                await using var stream = new FileStream(segmentFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
                while (stream.Position < stream.Length)
                {
                    var log = await ByteObjectConverter.ReadObjectFromStreamAsync<Log>(stream);
                    if (log is null) break;
                    if (seenLogIds.Add(log.Id))
                    {
                        yield return log;
                    }
                }
            }
        }
    }
}