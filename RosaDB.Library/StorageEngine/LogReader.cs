using System.IO.Abstractions;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.StorageEngine;

public class LogReader(
    SessionState sessionState,
    IFileSystem fileSystem,
    IFolderManager folderManager,
    IIndexManager indexManager,
    WriteAheadLogCache writeAheadLogCache) : ILogReader
{
    public async Task<Result<Log>> FindLastestLog(string contextName, string tableName, string instanceHash, long id)
    {
        var identifier = InstanceHasher.CreateIdentifier(contextName, tableName, instanceHash);
        
        if (writeAheadLogCache.Logs.TryGetValue(identifier, out var logs))
        {
            Log? log = logs.AsEnumerable().Reverse().FirstOrDefault(l => l.Id == id);
            if (log is not null) return log;
        }
        
        return await FindOnDisk(identifier, id);
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

        await using var stream = fileSystem.FileStream.New(segmentFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        stream.Seek(logLocation.Offset, SeekOrigin.Begin);
        
        var log = await LogSerializer.DeserializeAsync(stream);
        if (log is null) return new Error(ErrorPrefixes.FileError, "Failed to deserialize log from disk.");
        
        return log;
    }

    public async IAsyncEnumerable<Log> GetAllLogsForContextTable(string contextName, string tableName)
    {
        if (sessionState.CurrentDatabase is null) yield break;

        var tablePath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, contextName, tableName);
        if (!fileSystem.Directory.Exists(tablePath)) yield break;

        var dataFiles = fileSystem.Directory.GetFiles(tablePath, "*.dat", SearchOption.AllDirectories);

        var seenLogIds = new HashSet<long>();

        foreach (var dataFile in dataFiles)
        {
            await using var stream = fileSystem.FileStream.New(dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var logsInFile = new List<Log>();
            while (stream.Position < stream.Length)
            {
                var log = await LogSerializer.DeserializeAsync(stream);
                if (log is null) break;
                logsInFile.Add(log);
            }

            foreach (var log in logsInFile.AsEnumerable().Reverse())
            {
                if (seenLogIds.Add(log.Id) && !log.IsDeleted)
                    yield return log;
            }
        }
    }

    public async IAsyncEnumerable<Log> GetAllLogsForContextInstanceTable(string contextName, string tableName, string instanceHash)
    {
        if (sessionState.CurrentDatabase is null) throw new Exception();
        var identifier = InstanceHasher.CreateIdentifier(contextName, tableName, instanceHash);

        var allLogs = new List<Log>();

        if (writeAheadLogCache.Logs.TryGetValue(identifier, out var inMemoryLogs))
        {
            allLogs.AddRange(inMemoryLogs);
        }
        
        var dbPath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name);
        var instancePath = fileSystem.Path.Combine(dbPath, identifier.ContextName, identifier.TableName, identifier.InstanceHash.Substring(0, 2));

        if (fileSystem.Directory.Exists(instancePath))
        {
            var dataFiles = fileSystem.Directory.GetFiles(instancePath, $"{identifier.InstanceHash}_*.dat");

            foreach (var dataFile in dataFiles)
            {
                await using var stream = fileSystem.FileStream.New(dataFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
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
}