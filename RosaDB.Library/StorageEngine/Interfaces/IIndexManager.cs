using RosaDB.Library.Core;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IIndexManager
{
    public void Insert(string path, byte[] key, byte[] value);
    public void Delete(string path, byte[] key);
    public byte[] Get(string path, byte[] key);
}