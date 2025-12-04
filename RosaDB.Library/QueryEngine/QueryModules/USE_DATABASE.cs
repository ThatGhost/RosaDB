using RosaDB.Library.Core;
using RosaDB.Library.StateEngine;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.QueryEngine.QueryModules;

public class USE_DATABASE : QueryModule
{
    private string _name;

    public USE_DATABASE(string name)
    {
        _name = name;
    }
    
    public override async Task<Result> Execute(CancellationToken ct)
    {
        if(!await FolderManager.DoesFolderExist(_name)) return new Error(ErrorPrefixes.QueryExecutionError, "Folder does not exist");
        
        await StateManager.UpdateUsedDatabase(_name);
        return Result.Success();
    }
}