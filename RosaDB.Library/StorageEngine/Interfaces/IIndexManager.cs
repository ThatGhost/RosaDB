using CSharpTest.Net.Collections;
using RosaDB.Library.Core;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IIndexManager
{
    void Insert(TableInstanceIdentifier identifier, string columnName, byte[] key, LogLocation value);
    Result<LogLocation> Search(TableInstanceIdentifier identifier, string columnName, byte[] key);

    Result InsertCellData(string cellName, byte[] key, byte[] value);
    Result<byte[]> GetCellData(string cellName, byte[] key);
    Result<bool> CellDataExists(string cellName, byte[] key);

    Result InsertCellPropertyIndex(string cellName, string propertyName, byte[] key, byte[] value);
    Result<IEnumerable<KeyValuePair<byte[], byte[]>>> GetAllCellData(string cellName);
}