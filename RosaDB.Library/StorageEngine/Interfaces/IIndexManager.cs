using CSharpTest.Net.Collections;
using RosaDB.Library.Core;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IIndexManager
{
    void Insert(TableInstanceIdentifier identifier, string columnName, byte[] key, LogLocation value);
    Result<LogLocation> Search(TableInstanceIdentifier identifier, string columnName, byte[] key);
}