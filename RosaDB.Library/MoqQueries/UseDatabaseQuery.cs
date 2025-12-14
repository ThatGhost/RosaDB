using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class UseDatabaseQuery(RootManager rootManager, SessionState sessionState)
{
    public async Task Execute(string dbName)
    {
        var existingDbs = await rootManager.GetDatabaseNames();
        if (existingDbs.IsFailure) return;
        
        if (existingDbs.Value.Contains(dbName))
        {
            Database newDb = new Database(dbName, []);
            sessionState.CurrentDatabase = newDb;
        }
    }
}
