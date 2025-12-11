using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.QueryEngine.QueryModules;

public class CREATE_DATABASE(string[] parts) : QueryModule
{
    public override async Task<Result> Execute(CancellationToken ct)
    {
        string databaseName = parts[0];
        if (string.IsNullOrEmpty(databaseName))
        {
            return new Error(ErrorPrefixes.QueryExecutionError, "Database name not specified.");
        }

        return await EnvManager.CreateDatabase(databaseName);
    }
}