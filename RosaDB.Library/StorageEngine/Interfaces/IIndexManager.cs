using RosaDB.Library.Core;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IIndexManager
{
    public void Insert<T>(string path, T key, byte[] value);
    public void Insert<T>(string path, T key, object value);
    public void Update<T>(string path, T key, byte[] value);
    public void Update<T>(string path, T key, object value);
    public void Delete<T>(string path, T key);
    public byte[] Get<T>(string path, T key);
    public T GetLastKey<T>(string path);
    public T GetNextKey<T>(string path);
}