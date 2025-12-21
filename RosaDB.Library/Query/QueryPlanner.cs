using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.Queries;
using LightInject;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.Query;

public class QueryPlanner(
    DatabaseManager databaseManager, 
    CellManager cellManager, 
    LogManager logManager, 
    RootManager rootManager
    )
{
    public Result<IQuery> CreateQueryPlanFromTokens(string[] tokens)
    {
        if (tokens.Length <= 1) return new Error(ErrorPrefixes.QueryParsingError, "Empty query");

        switch (tokens[0].ToUpperInvariant())
        {
            case "CREATE": return new CreateQuery(tokens, rootManager, databaseManager, cellManager);
            default: return new Error(ErrorPrefixes.QueryParsingError, "Unknown query type");
        }
    }
}