using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.Queries;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using LightInject;

namespace RosaDB.Library.Query;

public class QueryPlanner(
    IDatabaseManager databaseManager, 
    IContextManager contextManager, 
    RootManager rootManager,
    SessionState sessionState,
    ILogManager logManager,
    IIndexManager indexManager,
    IServiceContainer serviceContainer
    )
{
    public Result<List<IQuery>> CreateQueryPlans(List<string[]> tokenLists)
    {
        var queryPlans = new List<IQuery>();
        var selectQueryCount = 0;

        foreach (var tokens in tokenLists)
        {
            if (tokens.Length > 0 && tokens[0].ToUpperInvariant() == "SELECT")
            {
                selectQueryCount++;
            }

            if (selectQueryCount > 1)
            {
                return new Error(ErrorPrefixes.QueryParsingError, "Only one SELECT query is allowed in a batch.");
            }

            var queryPlanResult = _CreateQueryPlanFromTokens(tokens);
            if (!queryPlanResult.TryGetValue(out var queryPlan))
            {
                return queryPlanResult.Error;
            }
            queryPlans.Add(queryPlan);
        }

        return queryPlans;
    }

    private Result<IQuery> _CreateQueryPlanFromTokens(string[] tokens)
    {
        if (tokens.Length < 1) return new Error(ErrorPrefixes.QueryParsingError, "Empty query.");

        return tokens[0].ToUpperInvariant() switch
        {
            "CREATE" => new CreateQuery(tokens, rootManager, databaseManager, contextManager),
            "DROP" => new DropQuery(tokens, rootManager, databaseManager, contextManager),
            "USE" => new UseQuery(tokens, sessionState, rootManager),
            "SELECT" => new SelectQuery(tokens, logManager, contextManager),
            "INSERT" => new InsertQuery(tokens, contextManager, logManager, indexManager, sessionState),
            "DELETE" => new DeleteQuery(tokens, contextManager, logManager, indexManager, sessionState),
            "INITIALIZE" => new InitializeQuery(rootManager, sessionState, serviceContainer),
            "ALTER" => new AlterQuery(tokens, contextManager),
            "BEGIN" => new BeginTransactionQuery(tokens, sessionState), 
            "COMMIT" => new CommitQuery(sessionState, logManager),
            "ROLLBACK" => new RollbackQuery(sessionState, logManager),
            _ => new Error(ErrorPrefixes.QueryParsingError, "Unknown query type"),
        };
    }
}