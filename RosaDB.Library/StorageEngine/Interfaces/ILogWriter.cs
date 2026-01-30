using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces;

public interface ILogWriter
{
    public void Rollback();
    public ValueTask<Result<Dictionary<long, LogLocation>>> Commit();
    public void Insert(string path, Row row, long logId);
    public void Update(string path, Row row, long logId);
    public void Delete(string path, long logId);
}