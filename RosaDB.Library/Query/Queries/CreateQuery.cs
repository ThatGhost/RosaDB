using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class CreateQuery(
    string[] tokens,
    RootManager rootManager,
    IDatabaseManager databaseManager)
    : IQuery
{
    public async Task<QueryResult> Execute()
    {
        if (tokens[0].ToUpperInvariant() != "CREATE") return new Error(ErrorPrefixes.QueryParsingError, "Invalid query type");

        switch (tokens[1].ToUpperInvariant())
        {
            case "DATABASE": return await CREATE_DATABASE(tokens[2]);
            case "CELL": return await CREATE_CELL(tokens[2], tokens[3..]);
            default: 
                return new Error(ErrorPrefixes.QueryParsingError, $"Unknown CREATE target: {tokens[1]}");
        }
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
        if (!columnResult.TryGetValue(out var column)) return columnResult.Error;
        
        var result = await databaseManager.CreateCell(cellName, column);
        if (result.IsFailure) return result.Error;
        
        return new QueryResult($"Successfully created cell: {cellName}");
    }
}