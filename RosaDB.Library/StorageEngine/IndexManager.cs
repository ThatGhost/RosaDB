using System.IO.Abstractions;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.StorageEngine;

public class IndexManager(IFileSystem fileSystem) : IIndexManager
{
    public void Insert<T>(string path, T key, byte[] value)
    {
        Task.Run(() =>
        {
            var options = GetOptions<T>(path);
            using var tree = new BPlusTree<T, byte[]>(options);
            tree[key] = value;
            tree.Commit();
        });
    }

    public void Insert<T>(string path, T key, object value) => Insert(path, key, ByteObjectConverter.ObjectToByteArray(value));

    public void Update<T>(string path, T key, byte[] value) => Insert(path, key, value);
    public void Update<T>(string path, T key, object value) => Update(path, key, ByteObjectConverter.ObjectToByteArray(value));
    
    public void Delete<T>(string path, T key)
    {
        Task.Run(() =>
        {
            if (!fileSystem.File.Exists(path))
            {
                return;
            }

            var options = GetOptions<T>(path);
            using var tree = new BPlusTree<T, byte[]>(options);
            tree.Remove(key);
            tree.Commit();
        });
    }

    public byte[] Get<T>(string path, T key)
    {
        var options = GetOptions<T>(path);

        using var tree = new BPlusTree<T, byte[]>(options);
        return tree.TryGetValue(key, out var value) ? value : [];
    }

    public T GetLastKey<T>(string path)
    {
        var options = GetOptions<T>(path);

        using var tree = new BPlusTree<T, byte[]>(options);
        return tree.Last().Key;
    }

    public T GetNextKey<T>(string path)
    {
        T lastKey = GetLastKey<T>(path);

        object nextKey = lastKey switch
        {
            short s => (short)(s + 1),
            int i => i + 1,
            long l  => l + 1,
            string s => s + "0",
            byte[] b => b.Concat([(byte)0]).ToArray(),
            _ => throw new NotSupportedException($"Incrementing type {typeof(T).Name} is not supported.")
        };

        return (T)nextKey;
    }

    private BPlusTree<T, byte[]>.OptionsV2 GetOptions<T>(string path)
    {
        return typeof(T) switch
        {
            { } t when t == typeof(long) => (BPlusTree<T, byte[]>.OptionsV2)(object)new BPlusTree<long, byte[]>.OptionsV2(PrimitiveSerializer.Int64, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            },

            { } t when t == typeof(string) => (BPlusTree<T, byte[]>.OptionsV2)(object)new BPlusTree<string, byte[]>.OptionsV2(PrimitiveSerializer.String, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            },

            { } t when t == typeof(int) => (BPlusTree<T, byte[]>.OptionsV2)(object)new BPlusTree<int, byte[]>.OptionsV2(PrimitiveSerializer.Int32, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            },

            { } t when t == typeof(short) => (BPlusTree<T, byte[]>.OptionsV2)(object)new BPlusTree<short, byte[]>.OptionsV2(PrimitiveSerializer.Int16, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            },
            
            _ => (BPlusTree<T, byte[]>.OptionsV2)(object)new BPlusTree<byte[], byte[]>.OptionsV2(PrimitiveSerializer.Bytes, PrimitiveSerializer.Bytes)
            {
                CreateFile = CreatePolicy.IfNeeded,
                FileName = path
            },
        };
    }
}