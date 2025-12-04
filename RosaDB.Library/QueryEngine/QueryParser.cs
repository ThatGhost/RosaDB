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

        if (queryParts.Length <= 2) return new Error(ErrorPrefixes.QueryParsingError, "Not enough parameters");

        switch (queryParts[0].ToUpper())
        {
            case "CREATE" : return CREATE(queryParts[1..]);
            case "USE" : return USE(queryParts[1..]);
        }
        
        return new Error(ErrorPrefixes.QueryParsingError, "Unknown query format");
    }

    private Result<QueryModule[]> CREATE(string[] queryParts)
    {
        switch (queryParts[0].ToUpper())
        {
            case "DATABASE":
                if(queryParts.Length == 1) return new Error(ErrorPrefixes.QueryParsingError, "No database name");
                return new QueryModule[] 
                    { new CREATE_DATABASE(queryParts[1]) };
        }
        
        return new Error(ErrorPrefixes.QueryParsingError, "Unknown CREATE query format");
    }

    private Result<QueryModule[]> USE(string[] queryParts)
    {
        if (queryParts.Length != 1) return new Error(ErrorPrefixes.QueryParsingError, "Database name missing");
        return new QueryModule[]
            { new USE_DATABASE(queryParts[0]) };
    }
}