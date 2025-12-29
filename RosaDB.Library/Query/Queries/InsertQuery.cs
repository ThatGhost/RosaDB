using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.Validation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

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

        return tokens[1].ToUpperInvariant() switch
        {
            "CELL" => InsertCellAsync(),
            "INTO" => Task.FromResult<QueryResult>(new Error(ErrorPrefixes.QueryParsingError, "INSERT INTO not yet implemented.")),
            _ => Task.FromResult<QueryResult>(new Error(ErrorPrefixes.QueryParsingError, $"Unknown INSERT target: {tokens[1]}")),
        };
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

        var schemaColumns = env.Columns;
        var valueMap = parsed.Props.Zip(parsed.Values, (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);

        var rowValues = new object?[schemaColumns.Length];
        var indexValues = new Dictionary<string, string>();

        for (int i = 0; i < schemaColumns.Length; i++)
        {
            var col = schemaColumns[i];
            if (valueMap.TryGetValue(col.Name, out var stringVal))
            {
                var parseValResult = StringToDataParser.Parse(stringVal, col.DataType);
                if (!parseValResult.TryGetValue(out var typedVal)) return parseValResult.Error;

                var validationResult = DataValidator.Validate(typedVal, col);
                if (validationResult.IsFailure) return validationResult.Error;

                rowValues[i] = typedVal;
                if (col.IsIndex)
                {
                    indexValues[col.Name] = stringVal;
                }
            }
            else if (!col.IsNullable) return new Error(ErrorPrefixes.DataError, $"Column '{col.Name}' is not nullable and must be provided.");
        }
        
        var requiredIndexCols = schemaColumns.Where(c => c.IsIndex).ToList();
        if (indexValues.Count != requiredIndexCols.Count)
        {
             var missingCols = string.Join(", ", requiredIndexCols.Select(c => c.Name).Except(indexValues.Keys));
             return new Error(ErrorPrefixes.DataError, $"INSERT CELL requires values for all indexed columns. Missing: {missingCols}");
        }

        // 3. GENERATE HASH
        var instanceHash = GenerateInstanceHash(schemaColumns, indexValues);

        // 4. CREATE ROW and SAVE
        var rowCreateResult = Row.Create(rowValues, schemaColumns);
        if (!rowCreateResult.TryGetValue(out var row))
            return rowCreateResult.Error;

        var saveResult = await cellManager.CreateCellInstance(parsed.CellGroupName, instanceHash, row, schemaColumns);
        if (saveResult.IsFailure)
            return saveResult.Error;

        return new QueryResult("1 cell instance inserted successfully.", 1);
    }

    private string GenerateInstanceHash(IEnumerable<Column> schemaColumns, IReadOnlyDictionary<string, string> indexValues)
    {
        var combinedIndex = string.Join("::", indexValues.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(combinedIndex)));
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