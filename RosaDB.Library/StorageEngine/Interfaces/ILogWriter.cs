using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogWriter
{
    public void Rollback();
    public ValueTask<Core.Result> Commit();
    public void Insert(string path, Row row);
    public void Update(string path, Row row, long logId);
    public void Delete(string path, long logId);
}