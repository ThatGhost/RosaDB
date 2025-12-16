using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class GetCellLogsQuery(LogManager logManager)
{
    public async Task<List<string>> Execute(string cellName, string tableName, object[] index)
    {
        Cell cell = new Cell(cellName);
        Table table = new Table() { Name = tableName };
        
        var logs = await logManager.GetAllLogsForCellInstanceTable(cell, table, index);
        if (logs.IsFailure) return [];

        List<string> data = new List<string>();
        foreach (var log in logs.Value)
        {
            var chars = ByteObjectConverter.ByteArrayToObject<char[]>(log.TupleData);
            if (chars != null)
            {
                data.Add(new string(chars));
            }
        }
        
        return data;
    }
}