using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class UseQuery(string[] tokens, SessionState sessionState, IDatabaseManager databaseManager) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (tokens[0].ToUpperInvariant() != "USE") return new Error(ErrorPrefixes.QueryParsingError, "Invalid query type");
        if (tokens.Length < 2) return new Error(ErrorPrefixes.QueryParsingError, "Not enough arguments for USE query");
        if (tokens.Length > 3) return new Error(ErrorPrefixes.QueryParsingError, "Too many arguments for USE query");

        string databaseName = tokens[1];

        var databaseResult = await databaseManager.GetDatabase(databaseName);
        if (databaseResult.IsFailure) return databaseResult.Error;

        sessionState.CurrentDatabase = databaseResult.Value;
        return new QueryResult($"Successfully using {databaseName}");
    }
}
