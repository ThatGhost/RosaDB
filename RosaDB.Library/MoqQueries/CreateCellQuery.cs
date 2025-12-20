using RosaDB.Library.Models;
using System.Collections.Generic;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class CreateCellQuery(DatabaseManager databaseManager)
{
    public async Task Execute(string cellName, List<Column>? columns = null)
    {
        if (columns == null)
        {
            var column = Column.Create("Id", DataType.BIGINT, isPrimaryKey: true, isIndex: true);
            if(column.IsFailure) return;
            columns = [column.Value];
        }
        await databaseManager.CreateCell(cellName, columns);
    }
}
