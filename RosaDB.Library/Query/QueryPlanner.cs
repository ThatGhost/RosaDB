using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.Queries;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query;

public class QueryPlanner(
    IDatabaseManager databaseManager, 
    ICellManager cellManager, 
    RootManager rootManager,
    SessionState sessionState,
    LogManager logManager,
    IIndexManager indexManager
    )
{
    public Result<IQuery> CreateQueryPlanFromTokens(string[] tokens)
    {
        if (tokens.Length <= 1) return new Error(ErrorPrefixes.QueryParsingError, "Empty query");

        switch (tokens[0].ToUpperInvariant())
        {
            case "CREATE": return new CreateQuery(tokens, rootManager, databaseManager);
            case "DROP": return new DropQuery(tokens, rootManager, databaseManager, cellManager);
            case "USE": return new UseQuery(tokens, sessionState);
            case "SELECT": return new SelectQuery(tokens, logManager, cellManager, indexManager);
            default: 
                return new Error(ErrorPrefixes.QueryParsingError, "Unknown query type");
        }
    }
}