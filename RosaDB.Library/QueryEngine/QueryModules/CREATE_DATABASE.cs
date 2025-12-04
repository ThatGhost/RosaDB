using RosaDB.Library.Core;
using RosaDB.Library.StorageEngine;
using RosaDB.Server;

namespace RosaDB.Library.QueryEngine.QueryModules;

public class CREATE_DATABASE : QueryModule
{
    private string _name;
    
    public CREATE_DATABASE(string name)
    {
        _name = name;
    }
    
    public override async Task<Result> Execute(ClientSession client, CancellationToken ct)
    {
        await FolderManager.CreateFolder(_name);

        return Result.Success();
    }
}