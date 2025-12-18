using System.Collections.Generic;
using System.Threading.Tasks;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class GetAllLogsQuery(LogManager logManager)
{
    public async Task<List<Log>> Execute(string cellName, string tableName)
    {
        Cell cell = new Cell(cellName);
        Table table = new Table() { Name = tableName };
        var logs = logManager.GetAllLogsForCellTable(cell, table);
        
        List<Log> result = new();
        await foreach (var log in logs)
        {
            result.Add(log);
        }
        
        return result;
    }
}