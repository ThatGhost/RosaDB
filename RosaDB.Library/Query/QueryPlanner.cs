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
    IModuleManager moduleManager, 
    SessionState sessionState,
    ILogReader logReader,
    ILogWriter logWriter,
    IIndexManager indexManager
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
            "CREATE" => new CreateQuery(tokens, databaseManager, moduleManager),
            "DROP" => new DropQuery(tokens, databaseManager, moduleManager),
            "USE" => new UseQuery(tokens, sessionState, databaseManager),
            "SELECT" => new SelectQuery(tokens, logReader, moduleManager),
            "INSERT" => new InsertQuery(tokens, moduleManager, logWriter, indexManager, sessionState),
            "DELETE" => new DeleteQuery(tokens, moduleManager, logReader, logWriter, sessionState),
            "INITIALIZE" => new InitializeQuery(databaseManager, sessionState),
            "ALTER" => new AlterQuery(tokens, moduleManager),
            "BEGIN" => new BeginTransactionQuery(tokens, sessionState), 
            "COMMIT" => new CommitQuery(sessionState, logWriter),
            "ROLLBACK" => new RollbackQuery(sessionState, logWriter),
            _ => new Error(ErrorPrefixes.QueryParsingError, "Unknown query type"),
        };
    }
}