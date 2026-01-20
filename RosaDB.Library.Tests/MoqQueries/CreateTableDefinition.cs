using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.MoqQueries;

public class CreateTableDefinition(IContextManager cellManager)
{
    public async Task<Result> Execute(string contextName, string tableName)
    {
        var tableResult = Table.Create(tableName, new[]
        {
            Column.Create("id", DataType.INT, isPrimaryKey: true).Value!,
            Column.Create("name", DataType.VARCHAR).Value!,
            Column.Create("age", DataType.INT).Value!,
        });

        if (tableResult.IsFailure) return tableResult.Error;
        
        var addTableResult = await cellManager.CreateTable(contextName, tableResult.Value);
        return addTableResult;
    }
}