using System.IO.Abstractions;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using RosaDB.Library.Core;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.StorageEngine;

public class IndexManager(
    IFileSystem fileSystem,
    IFolderManager folderManager) : IIndexManager
{
    private readonly Dictionary<string, BPlusTree<byte[], LogLocation>> _activeIndexes = new();

    private string GetIndexPath(TableInstanceIdentifier identifier, string columnName)
    {
        string path;
        if (identifier.InstanceHash == "_TABLE_")
        {
            path = fileSystem.Path.Combine(
                folderManager.BasePath,
                "indexes",
                identifier.CellName,
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
                "indexes",
                identifier.CellName,
                identifier.TableName,
                hashPrefix,
                $"{identifier.InstanceHash}_{columnName}.idx");
        }
        return path;
    }

    public BPlusTree<byte[], LogLocation> GetOrCreateBPlusTree(TableInstanceIdentifier identifier, string columnName)
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
        
        var options = new BPlusTree<byte[], LogLocation>.OptionsV2(PrimitiveSerializer.Bytes, new LogLocationSerializer())
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

    public void Insert(TableInstanceIdentifier identifier, string columnName, byte[] key, LogLocation value)
    {
        var btree = GetOrCreateBPlusTree(identifier, columnName);
        btree[key] = value;
    }

    public Result<LogLocation> Search(TableInstanceIdentifier identifier, string columnName, byte[] key)
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