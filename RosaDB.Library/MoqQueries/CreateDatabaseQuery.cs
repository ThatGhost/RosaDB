using RosaDB.Library.Core;
using RosaDB.Library.StorageEngine;
using System.Threading.Tasks;

namespace RosaDB.Library.MoqQueries;

public class CreateDatabaseQuery
{
    private readonly RootManager _rootManager;

    public CreateDatabaseQuery(RootManager rootManager)
    {
        _rootManager = rootManager;
    }

    public async Task<Result> Execute(string dbName)
    {
        try
        {
            return await _rootManager.CreateDatabase(dbName);
        }
        catch { return new CriticalError(); }
    }
}
