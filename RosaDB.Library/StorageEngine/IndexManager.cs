using System.IO.Abstractions;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.StorageEngine;

public class IndexManager(IFileSystem fileSystem) : IIndexManager
{
    public void Insert(string path, byte[] key, byte[] value)
    {
        Task.Run(() =>
        {
            var options = new BPlusTree<byte[], byte[]>.OptionsV2(PrimitiveSerializer.Bytes, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            };
            using var tree = new BPlusTree<byte[], byte[]>(options);
            tree[key] = value;
            tree.Commit();
        });
    }

    public void Delete(string path, byte[] key)
    {
        Task.Run(() =>
        {
            if (!fileSystem.File.Exists(path))
            {
                return;
            }

            var options = new BPlusTree<byte[], byte[]>.OptionsV2(PrimitiveSerializer.Bytes, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.Never,
                FileName = path
            };
            using var tree = new BPlusTree<byte[], byte[]>(options);
            tree.Remove(key);
            tree.Commit();
        });
    }

    public byte[] Get(string path, byte[] key)
    {
        var options = new BPlusTree<byte[], byte[]>.OptionsV2(PrimitiveSerializer.Bytes, PrimitiveSerializer.Bytes)
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = path
        };

        using var tree = new BPlusTree<byte[], byte[]>(options);
        return tree.TryGetValue(key, out var value) ? value : [];
    }
}