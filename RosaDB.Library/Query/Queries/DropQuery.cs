using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;
public class DropQuery(
    string[] tokens,
    RootManager rootManager,
    IDatabaseManager databaseManager,
    IContextManager cellManager) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (tokens[0].ToUpperInvariant() != "DROP") return new Error(ErrorPrefixes.QueryParsingError, "Invalid query type");
        if (tokens.Length < 3) return new Error(ErrorPrefixes.QueryParsingError, "Invalid amount of query parameters for DROP");
        
        return tokens[1].ToUpperInvariant() switch
        {
            "DATABASE" => await DROP_DATABASE(tokens[2]),
            "CONTEXT" => await DROP_CONTEXT(tokens[2]),
            "TABLE" => await DROP_TABLE(tokens[2..]),
            _ => new Error(ErrorPrefixes.QueryParsingError, "DROP type not supported"),
        };
    }

    private async Task<QueryResult> DROP_DATABASE(string name)
    {
        var result = await rootManager.DeleteDatabase(name);
        if (result.IsFailure) return result.Error;

        return new QueryResult($"Database: {name} was deleted successfully");
    }

    private async Task<QueryResult> DROP_CONTEXT(string name)
    {
        var result = await databaseManager.DeleteContext(name);
        if (result.IsFailure) return result.Error;

        return new QueryResult($"Context: {name} was deleted successfully");
    }

    private async Task<QueryResult> DROP_TABLE(string[] tableTokens)
    {
        string tableName = tableTokens[0];
        if (tableTokens[1] != "IN") return new Error(ErrorPrefixes.QueryParsingError, "Delete table does not define IN structure");
        string contextName = tableTokens[2];

        var result = await cellManager.DeleteTable(contextName, tableName);
        if(result.IsFailure) return result.Error;

        return new QueryResult($"Table with name: {tableName} in context: {contextName} was successfully dropped");
    }
}
