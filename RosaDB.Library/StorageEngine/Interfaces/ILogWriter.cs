namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogWriter : IAsyncDisposable
{
    ValueTask<Core.Result> Commit();
    void Rollback();
    void Put(string contextName, string tableName, object[] tableIndex, byte[] data, List<(string Name, byte[] Value, bool IsPrimaryKey)>? indexValues = null, long? logId = null);
    void Delete(string contextName, string tableName, object?[] indexValues, long logId);
}