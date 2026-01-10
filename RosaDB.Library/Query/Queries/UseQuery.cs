using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.Query.Queries;

public class UseQuery(string[] tokens, SessionState sessionState, RootManager rootManager) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (tokens[0].ToUpperInvariant() != "USE") return new Error(ErrorPrefixes.QueryParsingError, "Invalid query type");
        if (tokens.Length < 2) return new Error(ErrorPrefixes.QueryParsingError, "Not enough arguments for USE query");
        if (tokens.Length > 3) return new Error(ErrorPrefixes.QueryParsingError, "Too many arguments for USE query");

        string databaseName = tokens[1];

        var databaseNames = await rootManager.GetDatabaseNames();
        if (!databaseNames.TryGetValue(out var names) || !names.Contains(databaseName))
        {
            return new Error(ErrorPrefixes.QueryParsingError, $"Database '{databaseName}' does not exist.");
        }

        var database = Database.Create(databaseName);
        if (database.IsFailure) return database.Error;

        sessionState.CurrentDatabase = database.Value;
        return new QueryResult($"Successfully using {databaseName}");
    }
}
