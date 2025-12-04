using RosaDB.Library.Core;

namespace RosaDB.Library.QueryEngine;

public class QueryExecutor
{
    private readonly QueryParser _parser;

    public QueryExecutor()
    {
        _parser = new QueryParser();
    }
    
    public async Task<Result> Execute(string query, CancellationToken ct)
    {
        var modules = _parser.Parse(query);
        if (modules.IsFailure) return modules.Error;

        // TODO if one query fails all need to be reverted
        foreach (var queryModule in modules.Value)
        {
            var result = await queryModule.Excecute(ct);
            if (result.IsFailure) return result;
        }
        
        return Result.Success();
    }
}