using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using LightInject;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class InitializeQuery(IDatabaseManager databaseManager, SessionState sessionState) : IQuery
{
    private const string SystemDatabaseName = "_system";
    
    public async ValueTask<QueryResult> Execute()
    {
        return new Error(ErrorPrefixes.CriticalError, "Database initialization is not implemented.");
    }
}