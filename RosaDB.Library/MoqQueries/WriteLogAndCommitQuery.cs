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
        Cell dummyCell = new Cell(cell);
        Table dummyTable = new Table() { Name = table };
        var random = new Random();

        for (int i = 0; i < 10000; i++)
        {
            int instanceIndex = random.Next(0, 20);
            object[] indexValues = { instanceIndex };
            
            var payload = ($"{data} - {i}").ToCharArray();
            byte[] bytes = ByteObjectConverter.ObjectToByteArray(payload);

            logManager.Put(dummyCell, dummyTable, indexValues, bytes);
        }

        // Commit the log
        await logManager.Commit();
    }
}

