using RosaDB.Library.Models;
using System.Collections.Generic;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class CreateCellQuery(DatabaseManager databaseManager)
{
    public async Task Execute(string cellName, List<Column>? columns = null)
    {
        columns ??= [new Column("Id", DataType.BIGINT, isPrimaryKey: true, isIndex: true)];
        await databaseManager.CreateCell(cellName, columns);
    }
}
