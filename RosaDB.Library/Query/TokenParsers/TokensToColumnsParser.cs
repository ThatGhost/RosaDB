using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.Query.TokenParsers;

public static class TokensToColumnsParser
{
    public static Result<Column[]> TokensToColumns(string[] columnTokens)
    {
        if (columnTokens.Length == 0) return new Error(ErrorPrefixes.QueryParsingError, "No columns specified");

        var tokens = columnTokens.ToList();
        if (tokens[0] == "(") tokens.RemoveAt(0);
        if (tokens.Count > 0 && tokens[^1] == ";") tokens.RemoveAt(tokens.Count - 1);
        if (tokens.Count > 0 && tokens[^1] == ")") tokens.RemoveAt(tokens.Count - 1);

        List<List<string>> tokensPerColumn = [];
        List<string> currentColumn = [];
        foreach (var token in tokens)
        {
            if (token == ",")
            {
                tokensPerColumn.Add(currentColumn);
                currentColumn = [];
            }
            else
            {
                currentColumn.Add(token);
            }
        }
        tokensPerColumn.Add(currentColumn);

        List<Column> columns = [];
        foreach (var tokensInColumn in tokensPerColumn)
        {
            if (tokensInColumn.Count == 0) continue;
            var columnResult = TokensToColumn(tokensInColumn.ToArray());
            if (columnResult.IsFailure) return columnResult.Error;
            columns.Add(columnResult.Value);
        }

        return columns.ToArray();
    }

    public static Result<Column> TokensToColumn(string[] columnTokens)
    {
        if (columnTokens.Length <= 1) return new Error(ErrorPrefixes.QueryParsingError, "No column specified, missing datatype");
        
        string columnName = columnTokens[0];
        string typeName = columnTokens[1].ToUpperInvariant();
        int currentIndex = 2;

        object parameters = new { };
        
        if (columnTokens.Length > 2 && columnTokens[2] == "(")
        {
            var (paramResult, newIndex) = ParseParameters(columnTokens, currentIndex, typeName);
            if (paramResult.IsFailure) return paramResult.Error;
            parameters = paramResult.Value;
            currentIndex = newIndex;
        }

        var switchesResult = TokensToSwitches(columnTokens, currentIndex);
        if (switchesResult.IsFailure) return switchesResult.Error;
        var (isPrimaryKey, isIndex, isNullable) = switchesResult.Value;

        if (!Enum.TryParse<DataType>(typeName, true, out var dataType))
        {
            return new Error(ErrorPrefixes.QueryParsingError, $"Datatype '{typeName}' is unknown");
        }

        return Column.Create(columnName, dataType, parameters, isPrimaryKey, isIndex, isNullable);
    }
    
    private static (Result<object> result, int newIndex) ParseParameters(string[] tokens, int start, string typeName)
    {
        start++;
        if (start >= tokens.Length) return (new NoEndFound(typeName), -1);

        if (tokens.Length > start + 2 && tokens[start + 1] == ",") 
        {
            if (tokens.Length > start + 3 && tokens[start + 3] == ")")
            {
                if (!int.TryParse(tokens[start], out int p1) || !int.TryParse(tokens[start+2], out int p2))
                {
                    return (new CouldNotParse("parameters", typeName), -1);
                }
                return (new { firstParse = p1, secondParse = p2 }, start + 4);
            }
        }
        else if (tokens.Length > start + 1 && tokens[start + 1] == ")")
        {
            if (!int.TryParse(tokens[start], out int p1))
            {
                return (new CouldNotParse("parameter", typeName), -1);
            }
            return (new { parsingNumber = p1 }, start + 2);
        }

        return (new NoEndFound(typeName), -1);
    }

    private static Result<(bool isPrimaryKey, bool isIndex, bool isNullable)> TokensToSwitches(string[] tokens, int start)
    {
        bool isPrimaryKey = false;
        bool isIndex = false;
        bool isNullable = true;

        for (int i = start; i < tokens.Length; i++)
        {
            string currentToken = tokens[i].ToUpperInvariant();

            if (currentToken == "PRIMARY")
            {
                if (i + 1 >= tokens.Length || tokens[i + 1].ToUpperInvariant() != "KEY") 
                    return new Error(ErrorPrefixes.QueryParsingError, "PRIMARY defined without KEY keyword");

                isPrimaryKey = true;
                isNullable = false;
                i++;
            }
            else if (currentToken == "INDEX")
            {
                isIndex = true;
            }
            else if (currentToken == "NOT")
            {
                if (i + 1 >= tokens.Length || tokens[i + 1].ToUpperInvariant() != "NULL") 
                    return new Error(ErrorPrefixes.QueryParsingError, "NOT defined without NULL keyword");

                isNullable = false;
                i++;
            }
        }
        
        if (isPrimaryKey && isNullable) return new Error(ErrorPrefixes.QueryParsingError, "PRIMARY KEY constraint requires a column to be NOT NULL.");

        return (isPrimaryKey, isIndex, isNullable);
    }
    
#pragma warning disable CS9113
    private record NoBeginningFound(string Type) : Error(ErrorPrefixes.QueryParsingError, $"No beginning '(' found for {Type} type");
    private record NoEndFound(string Type) : Error(ErrorPrefixes.QueryParsingError, $"No end ')' found for {Type} type");
    private record CouldNotParse(string Value, string Type) : Error(ErrorPrefixes.QueryParsingError, $"Could not parse {Value} for {Type} type");
    private record NoCommaFound(string Type) : Error(ErrorPrefixes.QueryParsingError, $"Could not find comma separator for {Type} type");
#pragma warning restore CS9113
    
}
