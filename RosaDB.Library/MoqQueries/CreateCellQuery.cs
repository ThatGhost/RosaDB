using RosaDB.Library.Models;
using System.Collections.Generic;

namespace RosaDB.Library.MoqQueries;

public class CreateCellQuery
{
    public void Execute(string cellName)
    {
        var dummyColumns = new List<Column>
        {
            new Column("Id", DataType.BIGINT, isPrimaryKey: true)
        };
        Cell newCell = new Cell(cellName, dummyColumns);
        
        // In a real scenario, this cell would be passed to a manager
    }
}
