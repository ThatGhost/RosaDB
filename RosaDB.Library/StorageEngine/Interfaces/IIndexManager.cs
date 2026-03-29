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
    public U? Get<T, U>(string path, T key) where U : class;
    public U? GetStruct<T, U>(string path, T key) where U : struct;
    public T GetLastKey<T>(string path);
    public T GetNextKey<T>(string path);
}