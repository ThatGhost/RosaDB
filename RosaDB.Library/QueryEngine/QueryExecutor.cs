using RosaDB.Library.Core;
using RosaDB.Server;

namespace RosaDB.Library.QueryEngine;

public class QueryExecutor
{
    private readonly QueryParser _parser;

    public QueryExecutor()
    {
        _parser = new QueryParser();
    }
    
    // TODO if one query fails all need to be reverted
    public async Task<Result> Execute(ClientSession client, string query, CancellationToken ct)
    {
        var modules = _parser.Parse(query, client);
        if (modules.IsFailure) return modules.Error;

        foreach (var queryModule in modules.Value)
        {
            var result = await queryModule.Execute(ct);
            if (result.IsFailure) return result;
        }
        
        return Result.Success();
    }
}