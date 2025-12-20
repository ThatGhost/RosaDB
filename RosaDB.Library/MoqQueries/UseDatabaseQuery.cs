using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class UseDatabaseQuery(RootManager rootManager, SessionState sessionState, LogManager logManager)
{
    public async Task Execute(string dbName)
    {
        var existingDbs = await rootManager.GetDatabaseNames();
        if (existingDbs.IsFailure) return;
        
        if (existingDbs.Value.Contains(dbName))
        {
            var database = Database.Create(dbName);
            if(database.IsFailure) return;
            sessionState.CurrentDatabase = database.Value;
            await logManager.LoadIndexesAsync();
        }
    }
}
