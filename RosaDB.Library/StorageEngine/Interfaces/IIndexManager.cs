using CSharpTest.Net.Collections;
using RosaDB.Library.Core;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IIndexManager
{
    BPlusTree<long, LogLocation> GetOrCreateBPlusTree(TableInstanceIdentifier identifier, string columnName);
    void Insert(TableInstanceIdentifier identifier, string columnName, long key, LogLocation value);
    Result<LogLocation> Search(TableInstanceIdentifier identifier, string columnName, long key);
    void CloseAllIndexes();
}