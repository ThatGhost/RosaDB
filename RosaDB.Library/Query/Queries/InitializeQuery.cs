using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using LightInject;
using RosaDB.Library.Server.Logging;

namespace RosaDB.Library.Query.Queries;

public class InitializeQuery(RootManager rootManager, SessionState sessionState, IServiceContainer serviceContainer) : IQuery
{
    private const string SystemDatabaseName = "_system";
    
    public async ValueTask<QueryResult> Execute()
    {
        var rootResult = await rootManager.InitializeRoot()
            .ThenAsync(InitializeSystemDatabase)
            .ThenAsync(() => LogSystemInitializer.InitializeAsync(serviceContainer));

        return rootResult.IsSuccess ? new QueryResult("RosaDB successfully initialized") : rootResult.Error;
    }

    private async Task<Result> InitializeSystemDatabase()
    {
        return await rootManager.CreateDatabase(SystemDatabaseName)
            .Then(() => Database.Create(SystemDatabaseName))
            .Then(database =>
            {
                sessionState.CurrentDatabase = database;
                return Task.FromResult(Result.Success());
            });
    }
}