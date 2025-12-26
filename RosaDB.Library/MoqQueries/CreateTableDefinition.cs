using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.MoqQueries;

public class CreateTableDefinition(ICellManager cellManager)
{
    public async Task Execute(string cellName, string tableName)
    {
        var tableResult = Table.Create(tableName, new[]
        {
            Column.Create("id", DataType.INT, isPrimaryKey: true).Value!,
            Column.Create("name", DataType.VARCHAR).Value!,
            Column.Create("age", DataType.INT).Value!,
        });

        if (tableResult.IsSuccess)
        {
            await cellManager.AddTables(cellName, new[] { tableResult.Value });
        }
    }
}