using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class CreateTableDefinition(CellManager cellManager)
{
    public async Task Execute(string cellName, string tableName)
    {
        Cell cell = new Cell(cellName);
        cellManager.AddTables(cell, [
            new Table()
            {
                Name = tableName,
                Columns = [
                    new Column("data", DataType.VARCHAR, true)
                ]
            }
        ]);
    }
}