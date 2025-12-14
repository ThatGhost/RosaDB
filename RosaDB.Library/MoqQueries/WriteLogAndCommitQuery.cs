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
        // We need a dummy Cell and Table for the Put method
        // Since we don't have a real query execution context, we'll create simple ones.
        var dummyColumns = new List<Column>
        {
            new Column("Id", DataType.BIGINT, isPrimaryKey: true),
            new Column("Value", DataType.VARCHAR)
        };
        Cell dummyCell = new Cell("DummyCell", dummyColumns);
        Table dummyTable = new Table();
        byte[] dummyData = Encoding.UTF8.GetBytes("This is a dummy log entry.");

        // Write one log
        _logManager.Put(dummyCell, dummyTable, null, dummyData);

        // Commit the log
        await _logManager.Commit();
    }
}

