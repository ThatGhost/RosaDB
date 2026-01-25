using RosaDB.Library.Core;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IIndexManager
{
    void Insert(TableInstanceIdentifier identifier, string columnName, byte[] key, LogLocation value);
    Result<LogLocation> Search(TableInstanceIdentifier identifier, string columnName, byte[] key);

    Result InsertContextData(string contextName, byte[] key, byte[] value);
    Result<byte[]> GetContextData(string contextName, byte[] key);
    Result<bool> ContextDataExists(string contextName, byte[] key);

    Result InsertContextPropertyIndex(string contextName, string propertyName, byte[] key, byte[] value);
    Result<IEnumerable<KeyValuePair<byte[], byte[]>>> GetAllContextData(string contextName);
}