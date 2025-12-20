using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.MoqQueries;

public class WriteLogAndCommitQuery(LogManager logManager, CellManager cellManager)
{
    public async Task Execute(string cell, string table, string data)
    {
        var columns = await cellManager.GetColumnsFromTable(cell, table);
        if (columns.IsFailure) return;

        var random = new Random();

        for (int i = 0; i < 10000; i++)
        {
            object?[] rowValues = [i, $"{data}", random.Next(10, 100)];
            var row = Row.Create(rowValues, columns.Value);
            if(row.IsFailure) return;
            
            var bytes = RowSerializer.Serialize(row.Value);
            if(bytes.IsFailure) return;
            
            logManager.Put(cell, table, [random.Next(0,4)], bytes.Value);
        }

        // Commit the log
        await logManager.Commit();
    }
}

