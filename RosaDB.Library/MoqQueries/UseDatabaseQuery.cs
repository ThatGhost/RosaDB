using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class UseDatabaseQuery(RootManager rootManager, SessionState sessionState)
{
    public async Task<Result> Execute(string dbName)
    {
        var existingDbsResult = await rootManager.GetDatabaseNames();
        if (!existingDbsResult.TryGetValue(out var existingDbs)) return existingDbsResult.Error;
        
        if (existingDbs.Contains(dbName))
        {
            var database = Database.Create(dbName);
            if(database.IsFailure) return database.Error;
            sessionState.CurrentDatabase = database.Value;
        }

        return Result.Success();
    }
}
