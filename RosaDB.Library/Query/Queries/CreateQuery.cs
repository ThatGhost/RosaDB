using RosaDB.Library.Core;
using RosaDB.Library.Models;
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
        if (tokens.Length != 3 && tokens.Length != 4) return new Error(ErrorPrefixes.QueryParsingError, "Invalid number of parameters");
        if (tokens[0].ToUpperInvariant() != "CREATE") return new Error(ErrorPrefixes.QueryParsingError, "Invalid query type");

        switch (tokens[1].ToUpperInvariant())
        {
            case "DATABASE": return await CREATE_DATABASE(tokens[2]);
        }
        
        return new CriticalError();
    }

    private async Task<QueryResult> CREATE_DATABASE(string databaseName)
    {
        var result = await rootManager.CreateDatabase(databaseName);
        if (result.IsFailure) return result.Error!;

        return new QueryResult();
    }
}