namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogWriter : IAsyncDisposable
{
    ValueTask<Core.Result> Commit();
    void Rollback();
    void Put(string contextName, string tableName, object[] tableIndex, byte[] data, string instanceHash, long? logId = null);
    void Delete(string contextName, string tableName, string instanceHash, long logId);
}