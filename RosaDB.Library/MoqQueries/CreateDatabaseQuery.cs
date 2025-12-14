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

    public async Task Execute(string dbName)
    {
        await _rootManager.CreateDatabase(dbName);
    }
}
