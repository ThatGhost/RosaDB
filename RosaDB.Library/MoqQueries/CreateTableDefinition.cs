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
                    new Column("id", DataType.INT, true),
                    new Column("name", DataType.VARCHAR),
                    new Column("age", DataType.INT),
                ]
            }
        ]);
    }
}