using RosaDB.Library.Core;
using RosaDB.Library.StorageEngine;
using RosaDB.Server;

namespace RosaDB.Library.QueryEngine.QueryModules;

public class USE_DATABASE : QueryModule
{
    private string _name;

    public USE_DATABASE(string name)
    {
        _name = name;
    }
    
    public override async Task<Result> Execute(ClientSession client,CancellationToken ct)
    {
        var path = Path.Combine(client.DatabaseName, _name);
        if(!await FolderManager.DoesFolderExist(path))
            return new Error(ErrorPrefixes.QueryExecutionError, "Folder does not exist");
        
        client.SetDatabase(_name);
        return Result.Success();
    }
}