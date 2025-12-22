using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.Query.Queries;

public class CreateQuery(
    string[] tokens,
    RootManager rootManager,
    DatabaseManager databaseManager,
    CellManager cellManager)
    : IQuery
{
    public async Task<QueryResult> Execute()
    {
        if (tokens[0].ToUpperInvariant() != "CREATE") return new Error(ErrorPrefixes.QueryParsingError, "Invalid query type");

        switch (tokens[1].ToUpperInvariant())
        {
            case "DATABASE": return await CREATE_DATABASE(tokens[2]);
            case "CELL": return await CREATE_CELL(tokens[2], tokens[3..]);
        }
        
        return new CriticalError();
    }

    private async Task<QueryResult> CREATE_DATABASE(string databaseName)
    {
        var result = await rootManager.CreateDatabase(databaseName);
        if (result.IsFailure) return result.Error;

        return new QueryResult($"Successfully created database: {databaseName}");
    }

    private async Task<QueryResult> CREATE_CELL(string cellName, string[] columnTokens)
    {
        var columnResult = TokensToColumnsParser.TokensToColumns(columnTokens);
        if (columnResult.IsFailure) return columnResult.Error;
        
        var result = await databaseManager.CreateCell(cellName, columnResult.Value.ToList());
        if (result.IsFailure) return result.Error;
        
        return new QueryResult($"Successfully created cell: {cellName}");
    }
}