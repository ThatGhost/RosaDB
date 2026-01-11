using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class RollbackQuery(SessionState sessionState, ILogManager logManager) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (!sessionState.IsInTransaction)
        {
            return new Error(ErrorPrefixes.StateError, "No transaction is currently in progress.");
        }

        logManager.Rollback();
        sessionState.IsInTransaction = false;
        
        //This is a synchronous operation, but the interface requires an async method.
        await Task.CompletedTask;
        return new QueryResult("Transaction rolled back.");
    }
}
