using System.IO.Abstractions;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using RosaDB.Library.Core;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.StorageEngine;

public class IndexManager(
    IFileSystem fileSystem,
    IFolderManager folderManager,
    SessionState sessionState) : IIndexManager, IDisposable
{
    private readonly Dictionary<string, BPlusTree<byte[], LogLocation>> _activeIndexes = new();
    private readonly Dictionary<string, BPlusTree<byte[], byte[]>> _activeModuleInstanceStores = new();
    private readonly Dictionary<string, BPlusTree<byte[], byte[]>> _activeModulePropertyIndexes = new();

    public void Insert(TableInstanceIdentifier identifier, string columnName, byte[] key, LogLocation value)
    {
        var btree = GetOrCreateBPlusTree(identifier, columnName);
        btree[key] = value;
    }

    public Result<LogLocation> Search(TableInstanceIdentifier identifier, string columnName, byte[] key)
    {
        var indexKey = $"{identifier.ModuleName}_{identifier.TableName}_{identifier.InstanceHash}_{columnName}";

        if (!_activeIndexes.TryGetValue(indexKey, out var btree))
        {
            try { btree = GetOrCreateBPlusTree(identifier, columnName); }
            catch (Exception ex) { return new Error(ErrorPrefixes.DataError, $"Failed to open B+Tree for {indexKey}: {ex.Message}"); }
        }

        if (btree.TryGetValue(key, out var value)) return value;
        return new Error(ErrorPrefixes.DataError, $"Key '{key}' not found in index '{indexKey}'.");
    }

    public Result InsertModuleData(string moduleName, byte[] key, byte[] value)
    {
        var treeResult = GetOrCreateModuleInstanceStore(moduleName);
        if (treeResult.IsFailure) return treeResult.Error;

        var tree = treeResult.Value;
        tree[key] = value;
        tree.Commit();
        return Result.Success();
    }

    public Result<byte[]> GetModuleData(string moduleName, byte[] key)
    {
        var treeResult = GetOrCreateModuleInstanceStore(moduleName);
        if (treeResult.IsFailure) return treeResult.Error;

        if (treeResult.Value.TryGetValue(key, out var value)) return value;
        return new Error(ErrorPrefixes.DataError, "Module instance not found.");
    }

    public Result<bool> ModuleDataExists(string moduleName, byte[] key)
    {
        var treeResult = GetOrCreateModuleInstanceStore(moduleName);
        if (treeResult.IsFailure) return treeResult.Error;

        return treeResult.Value.ContainsKey(key);
    }

    public Result InsertModulePropertyIndex(string moduleName, string propertyName, byte[] key, byte[] value)
    {
        var treeResult = GetOrCreateModulePropertyIndex(moduleName, propertyName);
        if (treeResult.IsFailure) return treeResult.Error;

        var tree = treeResult.Value;
        tree[key] = value;
        tree.Commit();
        return Result.Success();
    }

    public Result<IEnumerable<KeyValuePair<byte[], byte[]>>> GetAllModuleData(string moduleName)
    {
        var treeResult = GetOrCreateModuleInstanceStore(moduleName);
        
        if (treeResult.IsFailure) return treeResult.Error;
        return treeResult.Value;
    }
    
    public void Dispose()
    {
        foreach (var btree in _activeIndexes.Values) btree.Dispose();
        foreach (var btree in _activeModuleInstanceStores.Values) btree.Dispose();
        foreach (var btree in _activeModulePropertyIndexes.Values) btree.Dispose();
        
        _activeIndexes.Clear();
        _activeModuleInstanceStores.Clear();
        _activeModulePropertyIndexes.Clear();
    }
    
    private string GetIndexPath(TableInstanceIdentifier identifier, string columnName)
    {
        string path;
        if (identifier.InstanceHash == "_TABLE_")
        {
            path = fileSystem.Path.Combine(
                folderManager.BasePath,
                sessionState.CurrentDatabase!.Name,
                "indexes",
                identifier.ModuleName,
                identifier.TableName,
                "_TABLE_", // Special folder for table-wide indexes
                $"{columnName}.idx"); // Simpler file name for table-wide indexes
        }
        else
        {
            var hashPrefix = identifier.InstanceHash.Length >= 2 
                ? identifier.InstanceHash.Substring(0, 2) 
                : "xy"; 

            path = fileSystem.Path.Combine(
                folderManager.BasePath,
                sessionState.CurrentDatabase!.Name,
                "indexes",
                identifier.ModuleName,
                identifier.TableName,
                hashPrefix,
                $"{identifier.InstanceHash}_{columnName}.idx");
        }
        return path;
    }

    private BPlusTree<byte[], LogLocation> GetOrCreateBPlusTree(TableInstanceIdentifier identifier, string columnName)
    {
        var indexKey = $"{identifier.ModuleName}_{identifier.TableName}_{identifier.InstanceHash}_{columnName}";

        if (_activeIndexes.TryGetValue(indexKey, out var btree)) return btree;

        var indexPath = GetIndexPath(identifier, columnName);
        var indexDirectory = fileSystem.Path.GetDirectoryName(indexPath);

        if (!string.IsNullOrEmpty(indexDirectory) && !fileSystem.Directory.Exists(indexDirectory)) fileSystem.Directory.CreateDirectory(indexDirectory);
        
        var options = new BPlusTree<byte[], LogLocation>.OptionsV2(PrimitiveSerializer.Bytes, new LogLocationSerializer(), new ByteArrayComparer())
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = indexPath,
            StoragePerformance = StoragePerformance.Default
        };
        options.CalcBTreeOrder(8, 12);

        btree = new BPlusTree<byte[], LogLocation>(options);
        _activeIndexes[indexKey] = btree;
        return btree;
    }

    private Result<BPlusTree<byte[], byte[]>> GetOrCreateModuleInstanceStore(string moduleName)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        
        var key = $"{sessionState.CurrentDatabase.Name}_{moduleName}";
        if (_activeModuleInstanceStores.TryGetValue(key, out var tree)) return tree;

        var filePath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, moduleName, "_idx");

        var options = new BPlusTree<byte[], byte[]>.OptionsV2(PrimitiveSerializer.Bytes, PrimitiveSerializer.Bytes, new ByteArrayComparer())
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = filePath
        };
        var newTree = new BPlusTree<byte[], byte[]>(options);
        _activeModuleInstanceStores[key] = newTree;
        return newTree;
    }

    private Result<BPlusTree<byte[], byte[]>> GetOrCreateModulePropertyIndex(string moduleName, string propertyName)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        
        var key = $"{sessionState.CurrentDatabase.Name}_{moduleName}_{propertyName}";
        if (_activeModulePropertyIndexes.TryGetValue(key, out var tree)) return tree;

        var filePath = fileSystem.Path.Combine(folderManager.BasePath, sessionState.CurrentDatabase.Name, moduleName, $"_pidx_{propertyName}");

        var options = new BPlusTree<byte[], byte[]>.OptionsV2(PrimitiveSerializer.Bytes, PrimitiveSerializer.Bytes, new ByteArrayComparer())
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = filePath
        };
        var newTree = new BPlusTree<byte[], byte[]>(options);
        _activeModulePropertyIndexes[key] = newTree;
        return newTree;
    }
}