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
    
    public async Task Execute()
    {
        // Example data - these would normally come from the query parsing
        Cell dummyCell = new Cell("DummyCell");
        Table dummyTable = new Table() { Name = "DummyTable" };
        byte[] dummyData = Encoding.UTF8.GetBytes("This is a dummy log entry.");
        object[] dummyIndex = { 1 };

        // Write one log
        _logManager.Put(dummyCell, dummyTable, dummyIndex, null, dummyData);

        // Commit the log
        await _logManager.Commit();
    }
}

