using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class CreateQuery(
    string[] tokens,
    RootManager rootManager,
    IDatabaseManager databaseManager,
    IContextManager contextManager)
    : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (tokens[0].ToUpperInvariant() != "CREATE") return new Error(ErrorPrefixes.QueryParsingError, "Invalid query type");

        return tokens[1].ToUpperInvariant() switch
        {
            "DATABASE" => await CREATE_DATABASE(tokens[2]),
            "CELL" => await CREATE_CELL(tokens[2], tokens[3..]),
            "TABLE" => await CREATE_TABLE(tokens[2], tokens[3..]),
            _ => new Error(ErrorPrefixes.QueryParsingError, $"Unknown CREATE target: {tokens[1]}")
        };
    }

    private async Task<QueryResult> CREATE_DATABASE(string databaseName)
    {
        var result = await rootManager.CreateDatabase(databaseName);
        return result.Match(
            () => new QueryResult($"Successfully created database: {databaseName}"),
            error => error
        );
    }

    private async Task<QueryResult> CREATE_CELL(string contextName, string[] columnTokens)
    {
        var columnResult = TokensToColumnsParser.TokensToColumns(columnTokens);
        
        return await columnResult.MatchAsync<QueryResult>(
            async columns =>
            {
                foreach (var c in columns) if (c.IsPrimaryKey) return new Error(ErrorPrefixes.QueryParsingError, "Primary key columns are not allowed. Use the INDEX keyword instead");
                var result = await databaseManager.CreateContext(contextName, columns);
                return result.Match(
                    () => new QueryResult($"Successfully created context: {contextName}"),
                    error => error
                );
            }, 
            error => Task.FromResult<QueryResult>(error)
        );
    }

    private async Task<QueryResult> CREATE_TABLE(string nameComposite, string[] columnTokens)
    {
        string[] names = nameComposite.Split('.');
        if (names.Length != 2) return new Error(ErrorPrefixes.QueryParsingError, "Invalid table name format, Expected: <contextName>.<tableName>");
        
        string contextName = names[0];
        string tableName = names[1];
        
        var columnResult = TokensToColumnsParser.TokensToColumns(columnTokens);
        
        return await columnResult.MatchAsync<QueryResult>(
            async columns =>
            {
                var tableResult = Table.Create(tableName, columns);
                if (!tableResult.TryGetValue(out var table)) return tableResult.Error;
                var result = await contextManager.CreateTable(contextName, table);
                return result.Match(
                    () => new QueryResult($"Successfully created Table: {tableName}"),
                    error => error
                );
            }, 
            error => Task.FromResult<QueryResult>(error)
        );
    }
}