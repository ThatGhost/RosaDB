using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;

namespace RosaDB.Library.Query.Queries;

public class UseQuery(string[] tokens, SessionState sessionState) : IQuery
{
    public async Task<QueryResult> Execute()
    {
        if (tokens[0].ToUpperInvariant() != "USE") return new Error(ErrorPrefixes.QueryParsingError, "Invalid query type");
        if (tokens.Length < 2) return new Error(ErrorPrefixes.QueryParsingError, "Not enough arguments for USE query");

        string databaseName = tokens[1];
        var database = Database.Create(databaseName);
        if (database.IsFailure) return database.Error;

        sessionState.CurrentDatabase = database.Value;
        return new QueryResult($"Succesfully using {databaseName}");
    }
}
