using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace RosaDB.Library.MoqQueries;

public class UseDatabaseQuery
{
    private readonly RootManager _rootManager;
    private readonly SessionState _sessionState;

    public UseDatabaseQuery(RootManager rootManager, SessionState sessionState)
    {
        _rootManager = rootManager;
        _sessionState = sessionState;
    }

    public async Task Execute(string dbName)
    {
        var existingDbs = await _rootManager.GetDatabaseNames();
        if (existingDbs.Contains(dbName))
        {
            Database newDb = new Database(dbName, Array.Empty<Cell>());
            _sessionState.CurrentDatabase = newDb;
        }
    }
}
