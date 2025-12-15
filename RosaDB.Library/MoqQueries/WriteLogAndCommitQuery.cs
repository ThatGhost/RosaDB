using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RosaDB.Library.MoqQueries;

public class WriteLogAndCommitQuery
{
    private readonly LogManager _logManager;

    public WriteLogAndCommitQuery(LogManager logManager)
    {
        _logManager = logManager;
    }
    
    public async Task Execute(string cell, string table, string data)
    {
        Cell dummyCell = new Cell(cell);
        Table dummyTable = new Table() { Name = table };
        var random = new System.Random();

        for (int i = 0; i < 10000; i++)
        {
            int instanceIndex = random.Next(0, 20);
            object[] indexValues = { instanceIndex };
            
            // Convert string to char[] and then serialize using the converter to match reader expectations
            var payload = ($"{data} - {i}").ToCharArray();
            byte[] bytes = ByteObjectConverter.ObjectToByteArray(payload);

            _logManager.Put(dummyCell, dummyTable, indexValues, bytes);
        }

        await _logManager.Commit();
    }
}

