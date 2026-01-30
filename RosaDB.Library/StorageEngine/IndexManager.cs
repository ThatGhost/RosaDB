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

    public void Update(string path, byte[] key, byte[] value) => Insert(path, key, value);

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

    public byte[] GetLastKey(string path)
    {
        var options = new BPlusTree<byte[], byte[]>.OptionsV2(PrimitiveSerializer.Bytes, PrimitiveSerializer.Bytes)
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = path
        };

        using var tree = new BPlusTree<byte[], byte[]>(options);
        return tree.Last().Key;
    }

    public void Insert(string path, int key, byte[] value)
    {
        Task.Run(() =>
        {
            var options = new BPlusTree<int, byte[]>.OptionsV2(PrimitiveSerializer.Int32, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            };
            using var tree = new BPlusTree<int, byte[]>(options);
            tree[key] = value;
            tree.Commit();
        });
    }

    public void Update(string path, int key, byte[] value) => Insert(path, key, value);

    public void Delete(string path, int key)
    {
        Task.Run(() =>
        {
            if (!fileSystem.File.Exists(path))
            {
                return;
            }

            var options = new BPlusTree<int, byte[]>.OptionsV2(PrimitiveSerializer.Int32, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            };
            using var tree = new BPlusTree<int, byte[]>(options);
            tree.Remove(key);
            tree.Commit();
        });
    }

    public byte[] Get(string path, int key)
    {
        var options = new BPlusTree<int, byte[]>.OptionsV2(PrimitiveSerializer.Int32, PrimitiveSerializer.Bytes)
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = path
        };
        using var tree = new BPlusTree<int, byte[]>(options);
        return tree.TryGetValue(key, out var value) ? value : [];
    }

    public int GetLastIntKey(string path)
    {
        var options = new BPlusTree<int, byte[]>.OptionsV2(PrimitiveSerializer.Int32, PrimitiveSerializer.Bytes)
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = path
        };
        using var tree = new BPlusTree<int, byte[]>(options);
        return tree.Last().Key;
    }
    
    public void Insert(string path, string key, byte[] value)
    {
        Task.Run(() =>
        {
            var options = new BPlusTree<string, byte[]>.OptionsV2(PrimitiveSerializer.String, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            };
            using var tree = new BPlusTree<string, byte[]>(options);
            tree[key] = value;
            tree.Commit();
        });
    }

    public void Update(string path, string key, byte[] value) => Insert(path, key, value);

    public void Delete(string path, string key)
    {
        Task.Run(() =>
        {
            if (!fileSystem.File.Exists(path))
            {
                return;
            }

            var options = new BPlusTree<string, byte[]>.OptionsV2(PrimitiveSerializer.String, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            };
            using var tree = new BPlusTree<string, byte[]>(options);
            tree.Remove(key);
            tree.Commit();
        });
    }

    public byte[] Get(string path, string key)
    {
        var options = new BPlusTree<string, byte[]>.OptionsV2(PrimitiveSerializer.String, PrimitiveSerializer.Bytes)
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = path
        };
        using var tree = new BPlusTree<string, byte[]>(options);
        return tree.TryGetValue(key, out var value) ? value : [];
    }

    public string GetLastStringKey(string path)
    {
        var options = new BPlusTree<string, byte[]>.OptionsV2(PrimitiveSerializer.String, PrimitiveSerializer.Bytes)
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = path
        };
        using var tree = new BPlusTree<string, byte[]>(options);
        return tree.Last().Key;
    }
    
    public void Insert(string path, long key, byte[] value)
    {
        Task.Run(() =>
        {
            var options = new BPlusTree<long, byte[]>.OptionsV2(PrimitiveSerializer.Int64, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            };
            using var tree = new BPlusTree<long, byte[]>(options);
            tree[key] = value;
            tree.Commit();
        });
    }

    public void Update(string path, long key, byte[] value) => Insert(path, key, value);

    public void Delete(string path, long key)
    {
        Task.Run(() =>
        {
            if (!fileSystem.File.Exists(path))
            {
                return;
            }

            var options = new BPlusTree<long, byte[]>.OptionsV2(PrimitiveSerializer.Int64, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            };
            using var tree = new BPlusTree<long, byte[]>(options);
            tree.Remove(key);
            tree.Commit();
        });
    }

    public byte[] Get(string path, long key)
    {
        var options = new BPlusTree<long, byte[]>.OptionsV2(PrimitiveSerializer.Int64, PrimitiveSerializer.Bytes)
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = path
        };
        using var tree = new BPlusTree<long, byte[]>(options);
        return tree.TryGetValue(key, out var value) ? value : [];
    }

    public long GetLastInt64Key(string path)
    {
        var options = new BPlusTree<long, byte[]>.OptionsV2(PrimitiveSerializer.Int64, PrimitiveSerializer.Bytes)
        {
            CreateFile = CreatePolicy.IfNeeded,
            FileName = path
        };
        using var tree = new BPlusTree<long, byte[]>(options);
        return tree.Last().Key;
    }
}