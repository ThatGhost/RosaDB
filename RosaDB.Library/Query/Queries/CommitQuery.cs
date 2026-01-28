using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class CommitQuery(SessionState sessionState, ILogWriter logWriter) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (!sessionState.IsInTransaction)
        {
            return new Error(ErrorPrefixes.StateError, "No transaction is currently in progress.");
        }

        var commitResult = await logWriter.Commit();
        if (commitResult.IsFailure)
        {
            return commitResult.Error;
        }

        sessionState.IsInTransaction = false;
        return new QueryResult("Transaction committed successfully.");
    }
}
