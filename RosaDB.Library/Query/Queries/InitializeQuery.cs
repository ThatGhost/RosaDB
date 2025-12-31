using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.Query.Queries;

public class InitializeQuery(RootManager rootManager) : IQuery
{
    public async Task<QueryResult> Execute()
    {
        var result = await rootManager.InitializeRoot();
        return result.IsFailure ? result.Error : new QueryResult("RosaDB successfully initialized");
    }
}