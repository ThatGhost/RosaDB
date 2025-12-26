using System.IO.Abstractions;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using RosaDB.Library.Core;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.StorageEngine;

public class IndexManager(
    IFileSystem fileSystem,
    IFolderManager folderManager) : IIndexManager
{
    private readonly Dictionary<string, BPlusTree<long, LogLocation>> _activeIndexes = new();

    private string GetIndexPath(TableInstanceIdentifier identifier, string columnName)
    {
        var hashPrefix = identifier.InstanceHash.Length >= 2 
            ? identifier.InstanceHash.Substring(0, 2) 
            : "xy"; 

        return fileSystem.Path.Combine(
            folderManager.BasePath,
            "indexes",
            identifier.CellName,
            identifier.TableName,
            hashPrefix,
            $"{identifier.InstanceHash}_{columnName}.idx");
    }

    public BPlusTree<long, LogLocation> GetOrCreateBPlusTree(TableInstanceIdentifier identifier, string columnName)
    {
        var indexKey = $"{identifier.CellName}_{identifier.TableName}_{identifier.InstanceHash}_{columnName}";

        if (_activeIndexes.TryGetValue(indexKey, out var btree))
        {
            return btree;
        }

        var indexPath = GetIndexPath(identifier, columnName);
        var indexDirectory = fileSystem.Path.GetDirectoryName(indexPath);

        if (!string.IsNullOrEmpty(indexDirectory) && !fileSystem.Directory.Exists(indexDirectory))
        {
            fileSystem.Directory.CreateDirectory(indexDirectory);
        }
        
        var options = new BPlusTree<long, LogLocation>.OptionsV2(PrimitiveSerializer.Int64, new LogLocationSerializer())
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = indexPath,
            StoragePerformance = StoragePerformance.Default
        };
        options.CalcBTreeOrder(8, 12);

        btree = new BPlusTree<long, LogLocation>(options);
        _activeIndexes[indexKey] = btree;
        return btree;
    }

    public void Insert(TableInstanceIdentifier identifier, string columnName, long key, LogLocation value)
    {
        var btree = GetOrCreateBPlusTree(identifier, columnName);
        btree[key] = value;
    }

    public Result<LogLocation> Search(TableInstanceIdentifier identifier, string columnName, long key)
    {
        var indexKey = $"{identifier.CellName}_{identifier.TableName}_{identifier.InstanceHash}_{columnName}";

        if (!_activeIndexes.TryGetValue(indexKey, out var btree))
        {
            try
            {
                btree = GetOrCreateBPlusTree(identifier, columnName);
            }
            catch (Exception ex)
            {
                return new Error(ErrorPrefixes.DataError, $"Failed to open B+Tree for {indexKey}: {ex.Message}");
            }
        }

        if (btree.TryGetValue(key, out var value))
        {
            return value;
        }

        return new Error(ErrorPrefixes.DataError, $"Key '{key}' not found in index '{indexKey}'.");
    }
    
    public void CloseAllIndexes()
    {
        foreach (var btree in _activeIndexes.Values)
        {
            btree.Dispose();
        }
        _activeIndexes.Clear();
    }
}