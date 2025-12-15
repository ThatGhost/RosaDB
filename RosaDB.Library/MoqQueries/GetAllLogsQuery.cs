using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class GetAllLogsQuery(LogManager logManager)
{
    public async Task<List<Log>> Execute(string cellName, string tableName)
    {
        Cell cell = new Cell(cellName);
        Table table = new Table() { Name = tableName };
        var logs = await logManager.GetAllLogsForCellTable(cell, table);
        if (logs.IsFailure) return [];
        
        return logs.Value;
    }
}