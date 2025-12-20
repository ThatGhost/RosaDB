using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class RandomDeleteLogsQuery(LogManager logManager)
{
    public async Task Execute(string cellName, string tableName, object[] indexValues)
    {
        var logs = logManager.GetAllLogsForCellInstanceTable(cellName, tableName, indexValues);
        var random = new Random();
        
        var idsToDelete = new List<long>();
        
        await foreach (var log in logs)
        {
            if (!log.IsDeleted && random.NextDouble() < 0.5)
            {
                idsToDelete.Add(log.Id);
            }
        }

        foreach (var id in idsToDelete)
        {
            logManager.Delete(cellName, tableName, indexValues, id);
        }

        await logManager.Commit();
    }
}
