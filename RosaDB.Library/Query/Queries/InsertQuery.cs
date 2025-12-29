using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using RosaDB.Library.Validation;
using System.Security.Cryptography;
using System.Text;

namespace RosaDB.Library.Query.Queries;

public class InsertQuery(
    string[] tokens,
    ICellManager cellManager,
    LogManager logManager) : IQuery
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
            "INTO" => InsertIntoAsync(),
            _ => Task.FromResult<QueryResult>(new Error(ErrorPrefixes.QueryParsingError, $"Unknown INSERT target: {tokens[1]}")),
        };
    }

    private async Task<QueryResult> InsertIntoAsync()
    {
        // 1. PARSE
        var parseResult = ParseInsertInto();
        if (!parseResult.TryGetValue(out var parsed))
            return parseResult.Error;

        // 2. FIND CELLINSTANCE HASH FROM USING CLAUSE
        var cellEnvResult = await cellManager.GetEnvironment(parsed.CellGroupName);
        if (!cellEnvResult.TryGetValue(out var cellEnv))
            return cellEnvResult.Error;

        var cellSchemaColumns = cellEnv.Columns;
        var usingIndexValues = new Dictionary<string, string>();

        foreach (var kvp in parsed.UsingProperties)
        {
            var col = cellSchemaColumns.FirstOrDefault(c => c.Name.Equals(kvp.Key, StringComparison.OrdinalIgnoreCase));
            if (col == null)
                return new Error(ErrorPrefixes.DataError, $"Property '{kvp.Key}' in USING clause does not exist in CellGroup '{parsed.CellGroupName}'.");
            if (!col.IsIndex)
                return new Error(ErrorPrefixes.DataError, $"Property '{kvp.Key}' in USING clause is not an indexed column for CellGroup '{parsed.CellGroupName}'.");

            usingIndexValues[col.Name] = kvp.Value;
        }

        if (usingIndexValues.Count != cellSchemaColumns.Where(c => c.IsIndex).Count())
            return new Error(ErrorPrefixes.DataError, $"USING clause requires values for all indexed CellGroup columns.");

        var cellInstanceHash = GenerateInstanceHash(cellSchemaColumns, usingIndexValues);

        var getCellInstanceResult = await cellManager.GetCellInstance(parsed.CellGroupName, cellInstanceHash);
        if (getCellInstanceResult.IsFailure) return getCellInstanceResult.Error;

        // 3. GET TABLE SCHEMA & VALIDATE DATA
        var tableSchemaResult = await cellManager.GetColumnsFromTable(parsed.CellGroupName, parsed.TableName);
        if (!tableSchemaResult.TryGetValue(out var tableSchemaColumns)) return tableSchemaResult.Error;
        
        var rowValueMap = parsed.Columns.Zip(parsed.Values, (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);

        var tableRowValues = new object?[tableSchemaColumns.Length];
        var tablePkValues = new List<object>();

        for (int i = 0; i < tableSchemaColumns.Length; i++)
        {
            var col = tableSchemaColumns[i];
            if (rowValueMap.TryGetValue(col.Name, out var stringVal))
            {
                var parseValResult = StringToDataParser.Parse(stringVal, col.DataType);
                if (!parseValResult.TryGetValue(out var typedVal)) return parseValResult.Error;

                var validationResult = DataValidator.Validate(typedVal, col);
                if (validationResult.IsFailure) return validationResult.Error;

                tableRowValues[i] = typedVal;
                if (col.IsPrimaryKey) tablePkValues.Add(typedVal!);
            }
            else if (!col.IsNullable)
                return new Error(ErrorPrefixes.DataError, $"Column '{col.Name}' is not nullable and must be provided for table '{parsed.TableName}'.");
        }

        if (tableSchemaColumns.Any(c => c.IsPrimaryKey) && tablePkValues.Count == 0)
            return new Error(ErrorPrefixes.DataError, $"Table '{parsed.TableName}' has primary key(s), but no values were provided in the INSERT INTO statement.");

        // 4. CREATE ROW AND LOG
        var tableRowCreateResult = Row.Create(tableRowValues, tableSchemaColumns);
        if (!tableRowCreateResult.TryGetValue(out var tableRow))
            return tableRowCreateResult.Error;

        var serializedRowResult = RowSerializer.Serialize(tableRow);
        if (!serializedRowResult.TryGetValue(out var serializedRowBytes))
            return serializedRowResult.Error;

        logManager.Put(parsed.CellGroupName, parsed.TableName, usingIndexValues.Values.Cast<object>().ToArray(), serializedRowBytes);
        
        // 5. COMMIT
        var commitResult = await logManager.Commit();
        return commitResult.IsFailure ? commitResult.Error : new QueryResult("1 row inserted successfully.", 1);
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

        var propsResult = ParseParenthesizedList(3, out int propsEnd);
        if (!propsResult.TryGetValue(out var props))
            return propsResult.Error;

        var valuesIndex = FindKeywordIndex("VALUES", propsEnd);
        if (valuesIndex == -1)
            return new Error(ErrorPrefixes.QueryParsingError, "Missing VALUES keyword.");

        var valuesResult = ParseParenthesizedList(valuesIndex + 1, out _);
        if (!valuesResult.TryGetValue(out var values))
            return valuesResult.Error;

        if (props.Length != values.Length)
            return new Error(ErrorPrefixes.QueryParsingError, "Property count does not match value count.");

        return (cellGroupName, props, values);
    }
    
    private Result<(string CellGroupName, string TableName, Dictionary<string, string> UsingProperties, string[] Columns, string[] Values)> ParseInsertInto()
    {
        // INSERT INTO <cellGroupName>.<tableName> USING (<prop1>=<val1>, ...) (<columns>) VALUES (<values>)
        if (tokens.Length < 10)
            return new Error(ErrorPrefixes.QueryParsingError, "Invalid INSERT INTO syntax. Expected: INSERT INTO <cell>.<table> USING (...) (...) VALUES (...)");

        return Result.Success()
            .Then(() => ParseTableNamePart(3))
            .Then(parsedTableName => ParseUsingClausePart(parsedTableName.NextIndex)
            .Then(parsedUsing => ParseColumnsPart(parsedUsing.NextIndex)
            .Then(parsedColumns => ParseValuesPart(parsedColumns.NextIndex)
            .Then<(string CellGroupName, string TableName, Dictionary<string, string> UsingProperties, string[] Columns, string[] Values)>(parsedValues =>
            {
                if (parsedColumns.Columns.Length != parsedValues.Values.Length)
                    return new Error(ErrorPrefixes.QueryParsingError, "Column count does not match value count.");

                return (
                    parsedTableName.CellGroupName,
                    parsedTableName.TableName,
                    parsedUsing.UsingProperties,
                    parsedColumns.Columns,
                    parsedValues.Values
                );
            }))));
    }

    private Result<(string CellGroupName, string TableName, int NextIndex)> ParseTableNamePart(int startIndex)
    {
        var fullTableName = tokens[startIndex -1].Split('.');
        if (fullTableName.Length != 2)
            return new Error(ErrorPrefixes.QueryParsingError, "Invalid table name format. Expected: <cellName>.<tableName>");
        
        var cellGroupName = fullTableName[0];
        var tableName = fullTableName[1];

        return (cellGroupName, tableName, startIndex);
    }

    private Result<(Dictionary<string, string> UsingProperties, int NextIndex)> ParseUsingClausePart(int startIndex)
    {
        var usingIndex = FindKeywordIndex("USING", startIndex);
        if (usingIndex == -1)
            return new Error(ErrorPrefixes.QueryParsingError, "Missing USING clause in INSERT INTO.");
        
        var usingResult = ParseUsingClause(usingIndex + 1, out int usingPropsEnd);
        if (!usingResult.TryGetValue(out var usingProperties))
            return usingResult.Error;

        return (usingProperties, usingPropsEnd);
    }
    
    private Result<(string[] Columns, int NextIndex)> ParseColumnsPart(int startIndex)
    {
        var columnsResult = ParseParenthesizedList(startIndex, out int columnsEnd);
        if(!columnsResult.TryGetValue(out var columns))
            return columnsResult.Error;

        return (columns, columnsEnd);
    }

    private Result<(string[] Values, int NextIndex)> ParseValuesPart(int startIndex)
    {
        var valuesIndex = FindKeywordIndex("VALUES", startIndex);
        if (valuesIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing VALUES keyword.");

        var valuesResult = ParseParenthesizedList(valuesIndex + 1, out int valuesEnd);
        if(!valuesResult.TryGetValue(out var values))
            return valuesResult.Error;

        return (values, valuesEnd);
    }

    private Result<Dictionary<string, string>> ParseUsingClause(int startIndex, out int endIndex)
    {
        endIndex = -1;
        var usingPropsResult = ParseParenthesizedList(startIndex, out endIndex);
        if (!usingPropsResult.TryGetValue(out var usingPropsTokens))
            return usingPropsResult.Error;

        var usingProperties = new Dictionary<string, string>();
        for (int i = 0; i < usingPropsTokens.Length; i += 3)
        {
            if (i + 2 >= usingPropsTokens.Length || usingPropsTokens[i+1] != "=")
                return new Error(ErrorPrefixes.QueryParsingError, "Malformed USING clause. Expected 'key=value' pairs.");
            usingProperties[usingPropsTokens[i]] = usingPropsTokens[i+2];
        }
        return usingProperties;
    }

    private Result<string[]> ParseParenthesizedList(int startIndex, out int endIndex)
    {
        endIndex = -1;
        if (startIndex >= tokens.Length) return new Error(ErrorPrefixes.QueryParsingError, "Unexpected end of query.");

        var openParenIndex = Array.IndexOf(tokens, "(", startIndex);
        if (openParenIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing opening parenthesis.");

        var closeParenIndex = Array.IndexOf(tokens, ")", openParenIndex);
        if (closeParenIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing closing parenthesis.");

        endIndex = closeParenIndex;
        var listTokens = tokens[(openParenIndex + 1)..closeParenIndex].Where(t => t != ",").ToArray();
        return listTokens;
    }

    private int FindKeywordIndex(string keyword, int startIndex = 0)
    {
        for (int i = startIndex; i < tokens.Length; i++)
        {
            if (tokens[i].Equals(keyword, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }
        return -1;
    }
}