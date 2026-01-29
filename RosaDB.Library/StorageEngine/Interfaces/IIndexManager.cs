using RosaDB.Library.Core;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IIndexManager
{
    void Insert(TableInstanceIdentifier identifier, string columnName, byte[] key, LogLocation value);
    Result<LogLocation> Search(TableInstanceIdentifier identifier, string columnName, byte[] key);

    Result InsertModuleData(string moduleName, byte[] key, byte[] value);
    Result<byte[]> GetModuleData(string moduleName, byte[] key);
    Result<bool> ModuleDataExists(string moduleName, byte[] key);

    Result InsertModulePropertyIndex(string moduleName, string propertyName, byte[] key, byte[] value);
    Result<IEnumerable<KeyValuePair<byte[], byte[]>>> GetAllModuleData(string moduleName);
}