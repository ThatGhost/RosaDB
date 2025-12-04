using RosaDB.Library.Core;
using RosaDB.Library.QueryEngine.QueryModules;

namespace RosaDB.Library.QueryEngine;

public class QueryParser
{
    // TODO support transactions
    // TODO support multi query strings
    public Result<QueryModule[]> Parse(string query)
    {
        string[] queryParts = query.Split([' ','\n']);

        if (queryParts.Length <= 2) return new Error("Query parsing error: Not enough parameters");

        switch (queryParts[0].ToUpper())
        {
            case "CREATE" : return CREATE(queryParts[1..]);
        }
        
        return new Error("Query parsing error: Unknown query format");
    }

    private Result<QueryModule[]> CREATE(string[] queryParts)
    {
        switch (queryParts[0].ToUpper())
        {
            case "DATABASE":
                if(queryParts.Length == 1) return new Error("Query parsing error: No database name");
                return new QueryModule[] {
                    new CREATE_DATABASE(queryParts[1])
                };
        }
        
        return new Error("Query parsing error: Unknown CREATE query format");
    }
}