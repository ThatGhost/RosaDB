using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using LightInject; // New using
using RosaDB.Library.Server.Logging; // New using

namespace RosaDB.Library.Query.Queries;

public class InitializeQuery(RootManager rootManager, SessionState sessionState, IServiceContainer serviceContainer) : IQuery
{
    private const string SystemDatabaseName = "_system";
    private const string SessionIdColumnName = "sessionId";
    private const string LogTableName = "_logs";

    public async ValueTask<QueryResult> Execute()
    {
        var rootResult = await rootManager.InitializeRoot()
            .ThenAsync(() => InitilizeSystemDatabase())
            .ThenAsync(() => LogSystemInitializer.InitializeAsync(serviceContainer));

        return rootResult.IsSuccess ? new QueryResult("RosaDB successfully initialized") : rootResult.Error;
    }

    private async Task<Result> InitilizeSystemDatabase()
    {
        return await rootManager.CreateDatabase(SystemDatabaseName)
            .Then<Database>(() => Database.Create(SystemDatabaseName))
            .Then(database =>
            {
                sessionState.CurrentDatabase = database;
                return Task.FromResult(Result.Success());
            });
    }
}