using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RosaDB.Library.MoqQueries;

public class WriteLogAndCommitQuery(LogManager logManager)
{
    public async Task Execute(string cell, string table, string data)
    {
        Column[] dummyColumns = [new Column("DataColumn", DataType.VARCHAR)];

        var random = new Random();

        for (int i = 0; i < 10000; i++)
        {
            int instanceIndex = random.Next(0, 20);
            object[] indexValues = { instanceIndex };
            
            object?[] rowValues = { $"{data} - {i}" };
            Row row = new Row(rowValues, dummyColumns);

            byte[] bytes = RowSerializer.Serialize(row);

            logManager.Put(cell, table, indexValues, bytes);
        }

        // Commit the log
        await logManager.Commit();
    }
}

