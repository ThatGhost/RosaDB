using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;

namespace RosaDB.Library.Query.Queries;

public class BeginTransactionQuery(string[] tokens, SessionState sessionState) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (tokens.Length < 2 || tokens[1].ToUpperInvariant() != "TRANSACTION") return new Error(ErrorPrefixes.QueryParsingError, "Invalid BEGIN syntax. Did you mean BEGIN TRANSACTION?");
        if (sessionState.IsInTransaction) return new Error(ErrorPrefixes.StateError, "A transaction is already in progress.");

        sessionState.IsInTransaction = true;
        return new QueryResult("Transaction started.");
    }
}
