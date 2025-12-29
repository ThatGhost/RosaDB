using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.Validation;
using System.Security.Cryptography;
using System.Text;

namespace RosaDB.Library.Query.Queries;

public class InsertQuery(
    string[] tokens,
    ICellManager cellManager) : IQuery
{
    public Task<QueryResult> Execute()
    {
        if (tokens.Length < 2)
        {
            return Task.FromResult<QueryResult>(new Error(ErrorPrefixes.QueryParsingError, "Invalid INSERT statement."));
        }

        switch (tokens[1].ToUpperInvariant())
        {
            case "CELL":
                return InsertCellAsync();
            case "INTO":
                // To be implemented
                return Task.FromResult<QueryResult>(new Error(ErrorPrefixes.QueryParsingError, "INSERT INTO not yet implemented."));
            default:
                return Task.FromResult<QueryResult>(new Error(ErrorPrefixes.QueryParsingError, $"Unknown INSERT target: {tokens[1]}"));
        }
    }

    private async Task<QueryResult> InsertCellAsync()
    {
        // 1. PARSE
        var parseResult = ParseInsertCell();
        if (!parseResult.TryGetValue(out var parsed))
            return parseResult.Error;

        // 2. GET SCHEMA & VALIDATE
        var envResult = await cellManager.GetEnvironment(parsed.CellGroupName);
        if (!envResult.TryGetValue(out var env))
            return envResult.Error;

        // Combines the properties and their corresponding schema columns
        Dictionary<string, string> valueMap = parsed.Props.Zip(parsed.Values, (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);

        var rowValues = new object?[env.Columns.Length];
        var pkValues = new List<string>();

        for (int i = 0; i < env.Columns.Length; i++)
        {
            Column col = env.Columns[i];
            if (valueMap.TryGetValue(col.Name, out var stringVal))
            {
                Result<object> parseValResult = StringToDataParser.Parse(stringVal, col.DataType);
                if (!parseValResult.TryGetValue(out var typedVal)) return parseValResult.Error;

                Result validationResult = DataValidator.Validate(typedVal, col);
                if (validationResult.IsFailure) return validationResult.Error;

                rowValues[i] = typedVal;
                if (col.IsPrimaryKey) pkValues.Add(stringVal);
            }
            else if (!col.IsNullable) 
                return new Error(ErrorPrefixes.DataError, $"Column '{col.Name}' is not nullable and must be provided.");
        }
        
        if (pkValues.Count == 0)
            return new Error(ErrorPrefixes.DataError, "INSERT CELL requires at least one primary key property.");

        // 3. GENERATE HASH
        var pkCombined = string.Join("::", pkValues.OrderBy(v => v));
        var instanceHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(pkCombined)));

        // 4. CREATE ROW and SAVE
        var rowCreateResult = Row.Create(rowValues, env.Columns);
        if (!rowCreateResult.TryGetValue(out var row)) return rowCreateResult.Error;

        var saveResult = await cellManager.CreateCellInstance(parsed.CellGroupName, instanceHash, row, env.Columns);
        if (saveResult.IsFailure) return saveResult.Error;

        return new QueryResult("1 cell instance inserted successfully.", 1);
    }

    private Result<(string CellGroupName, string[] Props, string[] Values)> ParseInsertCell()
    {
        // INSERT CELL <CellGroup> (<props>) VALUES (<vals>)
        if (tokens.Length < 6)
            return new Error(ErrorPrefixes.QueryParsingError, "Invalid INSERT CELL syntax.");

        var cellGroupName = tokens[2];

        var propsStart = Array.IndexOf(tokens, "(", 3);
        if (propsStart == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing opening parenthesis for properties.");
        
        var propsEnd = Array.IndexOf(tokens, ")", propsStart);
        if (propsEnd == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing closing parenthesis for properties.");

        var props = tokens[(propsStart + 1)..propsEnd].Where(t => t != ",").ToArray();

        var valuesIndex = Array.IndexOf(tokens, "VALUES", propsEnd);
        if(valuesIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing VALUES keyword.");

        var valuesStart = Array.IndexOf(tokens, "(", valuesIndex);
        if (valuesStart == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing opening parenthesis for values.");

        var valuesEnd = Array.IndexOf(tokens, ")", valuesStart);
        if (valuesEnd == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing closing parenthesis for values.");

        var values = tokens[(valuesStart + 1)..valuesEnd].Where(t => t != ",").ToArray();

        if (props.Length != values.Length)
            return new Error(ErrorPrefixes.QueryParsingError, "Property count does not match value count.");

        return (cellGroupName, props, values);
    }
}
