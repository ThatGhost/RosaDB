using System.Collections.Generic;
using System.Threading.Tasks;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class GetCellLogsQuery(LogManager logManager)
{
    public async Task<List<string>> Execute(string cellName, string tableName)
    {
        Cell cell = new Cell(cellName);
        Table table = new Table() { Name = tableName };
        
        // Retrieve all logs for the cell and table, across all instances
        var logs = logManager.GetAllLogsForCellTable(cell, table);

        List<string> data = new List<string>();
        await foreach (var log in logs)
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