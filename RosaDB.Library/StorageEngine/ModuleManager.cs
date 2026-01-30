using System.IO.Abstractions;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.StorageEngine;

public class ModuleManager(
    SessionState sessionState,
    ILogWriter logWriter,
    ILogReader logReader,
    IIndexManager indexManager,
    IFolderManager folderManager,
    IFileSystem fileSystem) : IModuleManager
{
    private readonly Dictionary<string, string> _moduleCurrentDataPaths = new();

    public async Task<Result> CreateModuleInstance(string moduleName, Row instanceData)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        var module = sessionState.CurrentDatabase.GetModule(moduleName);
        if (module is null) return new Error(ErrorPrefixes.FileError, "Module not found");

        string modulePath = GetModulePath(moduleName);
        string moduleIndexPath = fileSystem.Path.Combine(modulePath, $"hash.idx");
        string moduleLogIndexPath = fileSystem.Path.Combine(modulePath, $"log.idx");

        if (indexManager.Get(moduleIndexPath, instanceData.InstanceHash).Length != 0)
            return new Error(ErrorPrefixes.FileError, "A module with these indexes already exists");
        long lastLogId = indexManager.GetLastInt64Key(moduleLogIndexPath);
        long newLogId = lastLogId + 1;

        string moduleDataPath = GetCurrentDataPath(moduleName, modulePath);
        indexManager.Insert(moduleLogIndexPath, newLogId, []); // reserve the logId

        logWriter.Insert(moduleDataPath, instanceData, newLogId);
        var commitResult = await logWriter.Commit();

        if (commitResult.IsFailure || !commitResult.Value.TryGetValue(newLogId, out LogLocation logLocation))
        {
            indexManager.Delete(moduleLogIndexPath, newLogId);
            return new Error(ErrorPrefixes.FileError, "Module could not be created");
        }

        var locationBytes = ByteObjectConverter.ObjectToByteArray(logLocation);
        indexManager.Insert(moduleIndexPath, instanceData.InstanceHash, locationBytes);
        indexManager.Update(moduleLogIndexPath, newLogId, locationBytes);

        return Result.Success();
    }

    public async Task<Result<Row>> GetModuleInstance(string moduleName, string instanceHash)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        var module = sessionState.CurrentDatabase.GetModule(moduleName);
        if (module is null) return new Error(ErrorPrefixes.FileError, "Module not found");

        string modulePath = GetModulePath(moduleName);
        string moduleIndexPath = fileSystem.Path.Combine(modulePath, $"hash.idx");

        byte[] logLocationBytes = indexManager.Get(moduleIndexPath, instanceHash);
        if (logLocationBytes.Length == 0)
            return new Error(ErrorPrefixes.FileError, "A module with these indexes does not exist");

        if (ByteObjectConverter.ByteArrayToStruct<LogLocation>(logLocationBytes) is not { } location)
            return new Error(ErrorPrefixes.FileError, "Module could not be loaded");

        return await logReader.FindLog(location)
            .Then(log => RowSerializer.Deserialize(log.TupleData, module.Columns.ToArray()));
    }

    public async Task<Result> DeleteModuleInstance(string moduleName, string instanceHash)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        var module = sessionState.CurrentDatabase.GetModule(moduleName);
        if (module is null) return new Error(ErrorPrefixes.FileError, "Module not found");

        string modulePath = GetModulePath(moduleName);
        string moduleIndexPath = fileSystem.Path.Combine(modulePath, $"hash.idx");
        string moduleLogIndexPath = fileSystem.Path.Combine(modulePath, $"log.idx");
        string moduleDataPath = GetCurrentDataPath(moduleName, modulePath);

        byte[] logLocationBytes = indexManager.Get(moduleIndexPath, instanceHash);
        if (logLocationBytes.Length == 0)
            return new Error(ErrorPrefixes.FileError, "A module with these indexes does not exist");

        if (ByteObjectConverter.ByteArrayToStruct<LogLocation>(logLocationBytes) is not { } location)
            return new Error(ErrorPrefixes.FileError, "Module could not be loaded");

        logWriter.Delete(moduleDataPath, location.logId);
        await logWriter.Commit();

        indexManager.Delete(moduleLogIndexPath, location.logId);
        indexManager.Delete(moduleIndexPath, instanceHash);

        return Result.Success();
    }

    public async IAsyncEnumerable<Row> GetAllModuleInstances(string moduleName)
    {
        var module = sessionState.CurrentDatabase?.GetModule(moduleName);
        if (module is null) yield break;

        string modulePath = GetModulePath(moduleName);
        HashSet<long> seenLogIds = [];
        foreach (var datFile in GetAllDatFiles(modulePath))
        {
            await foreach (var log in logReader.GetAllLogs(datFile))
            {
                if (log.IsDeleted)
                {
                    seenLogIds.Add(log.Id);
                    continue;
                }

                if (seenLogIds.Contains(log.Id)) continue;

                seenLogIds.Add(log.Id);
                if (RowSerializer.Deserialize(log.TupleData, module.Columns.ToArray()).TryGetValue(out var row))
                    yield return row;
            }
        }
    }

    private string GetModulePath(string module) => fileSystem.Path.Combine(folderManager.BasePath, module);

    private IEnumerable<string> GetAllDatFiles(string path) => fileSystem.Directory.EnumerateFiles(path, "*.dat");

    private string GetCurrentDataPath(string moduleName, string modulePath)
    {
        if (_moduleCurrentDataPaths.TryGetValue(moduleName, out var cachedPath))
        {
            var cachedFileInfo = fileSystem.FileInfo.New(cachedPath);
            if (cachedFileInfo is { Exists: true, Length: < 1_000_000 }) return cachedPath;
        }

        var highestFile = GetAllDatFiles(modulePath)
            .Select(p => (path: p, num: int.Parse(fileSystem.Path.GetFileNameWithoutExtension(p))))
            .OrderByDescending(f => f.num)
            .FirstOrDefault();

        if (highestFile == default)
        {
            string newPath = fileSystem.Path.Combine(modulePath, "0.dat");
            _moduleCurrentDataPaths[moduleName] = newPath;
            return newPath;
        }
        
        var fileInfo = fileSystem.FileInfo.New(highestFile.path);
        return fileInfo is { Exists: true, Length: < 1_000_000 } ? 
            highestFile.path : 
            fileSystem.Path.Combine(modulePath, $"{highestFile.num + 1}.dat");
    }
}
