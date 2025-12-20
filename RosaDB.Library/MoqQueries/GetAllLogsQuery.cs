using System.Collections.Generic;
using System.Threading.Tasks;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class GetAllLogsQuery(LogManager logManager)
{
    public async Task<List<Log>> Execute(string cellName, string tableName)
    {
        var logs = logManager.GetAllLogsForCellTable(cellName, tableName);
        
        List<Log> result = new();
        await foreach (var log in logs)
        {
            result.Add(log);
        }
        
        return result;
    }
}