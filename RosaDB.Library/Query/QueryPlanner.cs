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
    ILogManager logManager,
    IIndexManager indexManager
    )
{
    public Result<IQuery> CreateQueryPlanFromTokens(string[] tokens)
    {
        if (tokens.Length <= 1) return new Error(ErrorPrefixes.QueryParsingError, "Empty query");

        return tokens[0].ToUpperInvariant() switch
        {
            "CREATE" => new CreateQuery(tokens, rootManager, databaseManager, cellManager),
            "DROP" => new DropQuery(tokens, rootManager, databaseManager, cellManager),
            "USE" => new UseQuery(tokens, sessionState, rootManager),
            "SELECT" => new SelectQuery(tokens, logManager, cellManager),
            "INSERT" => new InsertQuery(tokens, cellManager, logManager, indexManager, sessionState),
            "INITIALIZE" => new InitializeQuery(rootManager),
            "ALTER" => new AlterQuery(tokens, cellManager),
            "BEGIN" => new BeginTransactionQuery(tokens, sessionState), 
            "COMMIT" => new CommitQuery(sessionState, logManager),
            "ROLLBACK" => new RollbackQuery(sessionState, logManager),
            _ => new Error(ErrorPrefixes.QueryParsingError, "Unknown query type"),
        };
    }
}