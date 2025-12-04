using RosaDB.Library.Core;
using RosaDB.Library.StorageEngine;
using RosaDB.Server;

namespace RosaDB.Library.QueryEngine.QueryModules;

public class USE_DATABASE(string name, ClientSession client) : QueryModule
{
    public override async Task<Result> Execute(CancellationToken ct)
    {
        var path = Path.Combine(client.DatabaseName, name);
        if(!await FolderManager.DoesFolderExist(path))
            return new Error(ErrorPrefixes.QueryExecutionError, "Folder does not exist");
        
        client.SetDatabase(name);
        return Result.Success();
    }
}