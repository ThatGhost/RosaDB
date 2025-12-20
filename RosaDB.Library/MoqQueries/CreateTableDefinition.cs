using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class CreateTableDefinition(CellManager cellManager)
{
    public async Task Execute(string cellName, string tableName)
    {
        await cellManager.AddTables(cellName, [
            new Table()
            {
                Name = tableName,
                Columns = [
                    Column.Create("id", DataType.INT, true).Value,
                    Column.Create("name", DataType.VARCHAR).Value,
                    Column.Create("age", DataType.INT).Value,
                ]
            }
        ]);
    }
}