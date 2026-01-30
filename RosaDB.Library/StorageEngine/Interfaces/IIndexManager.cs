using RosaDB.Library.Core;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IIndexManager
{
    // byte[] keys
    public void Insert(string path, byte[] key, byte[] value);
    public void Update(string path, byte[] key, byte[] value);
    public void Delete(string path, byte[] key);
    public byte[] Get(string path, byte[] key);
    public byte[] GetLastKey(string path);
    
    // int keys
    public void Insert(string path, int key, byte[] value);
    public void Update(string path, int key, byte[] value);
    public void Delete(string path, int key);
    public byte[] Get(string path, int key);
    public int GetLastIntKey(string path);
    
    // string keys
    public void Insert(string path, string key, byte[] value);
    public void Update(string path, string key, byte[] value);
    public void Delete(string path, string key);
    public byte[] Get(string path, string key);
    public string GetLastStringKey(string path);
    
    // long keys
    public void Insert(string path, long key, byte[] value);
    public void Update(string path, long key, byte[] value);
    public void Delete(string path, long key);
    public byte[] Get(string path, long key);
    public long GetLastInt64Key(string path);
}