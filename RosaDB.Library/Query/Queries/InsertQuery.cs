using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using RosaDB.Library.Server;

namespace RosaDB.Library.Query.Queries;

// TODO complete redo
public class InsertQuery(
    string[] tokens,
    IModuleManager cellManager,
    ILogWriter logWriter,
    IIndexManager indexManager,
    SessionState sessionState) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (tokens.Length < 3) return new Error(ErrorPrefixes.QueryParsingError, "Invalid INSERT statement.");

        return tokens[1].ToUpperInvariant() switch
        {
            "MODULE" => await InsertModuleAsync(),
            "INTO" => await InsertIntoAsync(),
            _ => new Error(ErrorPrefixes.QueryParsingError, $"Unknown INSERT target: {tokens[1]}"),
        };
    }

    private async Task<QueryResult> InsertIntoAsync()
    {
        return await ParseInsertInto()
            .ThenAsync(parsed => AssertUsingClause(parsed)
            .ThenAsync(usingIndexValues => cellManager.GetColumnsFromTable(parsed.ModuleGroupName, parsed.TableName)
            .ThenAsync(async tableSchemaColumns =>
            {
                var rowValueMap = parsed.Columns.Zip(parsed.Values, (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);
                
                var rowResult = GetRowValues(rowValueMap, tableSchemaColumns, parsed.TableName)
                    .Then(tableRowValues => Row.Create(tableRowValues, tableSchemaColumns))
                    .Then(tableRow => CheckPrimaryKeys(parsed.ModuleGroupName, parsed.TableName, tableRow.InstanceHash, tableRow));
                
                if (rowResult.IsFailure) return rowResult.Error;
                var row = rowResult.Value;

                logWriter.Put(parsed.ModuleGroupName, parsed.TableName, usingIndexValues, row.BSON, row.InstanceHash);
                
                if (!sessionState.IsInTransaction) return await logWriter.Commit();

                return Result.Success();
            })))
            .MatchAsync(
                () => Task.FromResult(new QueryResult("1 row inserted successfully.", 1)),
                error => Task.FromResult<QueryResult>(error)
            );
    }
    
    private Result<Row> CheckPrimaryKeys(string moduleName, string tableName, string instanceHash, Row tableRow)
    {
        var identifier = InstanceHasher.CreateIdentifier(moduleName, tableName, instanceHash);

        foreach (var col in tableRow.Columns)
        {
            if (col.IsPrimaryKey)
            {
                var val = tableRow.GetValue(col.Name);
                if (val == null) continue; // Should not happen for PK, but safe check
                
                var keyBytes = IndexKeyConverter.ToByteArray(val);
                var searchResult = indexManager.Search(identifier, col.Name, keyBytes);
                
                if (searchResult.IsSuccess)
                {
                     return new Error(ErrorPrefixes.DataError, $"Duplicate primary key value '{val}' for column '{col.Name}'.");
                }
            }
        }
        return tableRow;
    }

    private async Task<QueryResult> InsertModuleAsync()
    {
        // 1. PARSE
        var parseResult = ParseInsertModule();
        if (!parseResult.TryGetValue(out var parsed)) return parseResult.Error;

        // 2. GET SCHEMA & VALIDATE
        var envResult = await cellManager.GetEnvironment(parsed.ModuleGroupName);
        if (!envResult.TryGetValue(out var cellEnv)) return envResult.Error;

        var schemaColumns = cellEnv.Columns;
        var valueMap = parsed.Props.Zip(parsed.Values, (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);

        var rowValues = new object?[schemaColumns.Length];
        var indexValues = new Dictionary<string, string>();

        for (int i = 0; i < schemaColumns.Length; i++)
        {
            var addResult = AddValuesToCollections(schemaColumns[i], valueMap, rowValues, indexValues, i);
            if (addResult.IsFailure) return addResult.Error;
        }
        
        var requiredIndexCols = schemaColumns.Where(c => c.IsIndex).ToList();
        if (indexValues.Count != requiredIndexCols.Count)
        {
             var missingCols = string.Join(", ", requiredIndexCols.Select(c => c.Name).Except(indexValues.Keys));
             return new Error(ErrorPrefixes.DataError, $"INSERT MODULE requires values for all indexed columns. Missing: {missingCols}");
        }

        // 3. GENERATE HASH
        var instanceHash = InstanceHasher.GenerateModuleInstanceHash(indexValues);

        // 4. CREATE ROW and SAVE
        var rowCreateResult = Row.Create(rowValues, schemaColumns);
        if (!rowCreateResult.TryGetValue(out var row)) return rowCreateResult.Error;

        var saveResult = cellManager.CreateModuleInstance(parsed.ModuleGroupName, instanceHash, row, schemaColumns);
        if (saveResult.IsFailure) return saveResult.Error;

        return new QueryResult("1 module instance inserted successfully.", 1);
    }

    private Result AddValuesToCollections(Column col, Dictionary<string, string> valueMap, object?[] rowValues, Dictionary<string, string> indexValues, int i)
    {
        if (valueMap.TryGetValue(col.Name, out var stringVal))
        {
            var parseValResult = TokensToDataParser.Parse(stringVal, col.DataType);
            if (!parseValResult.TryGetValue(out var typedVal)) return parseValResult.Error;

            var validationResult = DataValidator.Validate(typedVal, col);
            if (validationResult.IsFailure) return validationResult.Error;

            rowValues[i] = typedVal;
            if (col.IsIndex) indexValues[col.Name] = stringVal;
        }
        else if (!col.IsNullable) return new Error(ErrorPrefixes.DataError, $"Column '{col.Name}' is not nullable and must be provided.");
        return Result.Success();
    }

    private Result<(string ModuleGroupName, string[] Props, string[] Values)> ParseInsertModule()
    {
        // INSERT MODULE <ModuleGroup> (<props>) VALUES (<vals>)
        if (tokens.Length < 6) return new Error(ErrorPrefixes.QueryParsingError, "Invalid INSERT MODULE syntax.");

        var moduleName = tokens[2];

        var propsResult = TokensToParenthesizedList.ParseParenthesizedList(tokens, 3, out int propsEnd);
        if (!propsResult.TryGetValue(out var props))
            return propsResult.Error;

        var valuesIndex = TokenToKeywordIndex.Find(tokens, "VALUES", propsEnd);
        if (valuesIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing VALUES keyword.");

        var valuesResult = TokensToParenthesizedList.ParseParenthesizedList(tokens, valuesIndex + 1, out _);
        if (!valuesResult.TryGetValue(out var values)) return valuesResult.Error;

        if (props.Length != values.Length) return new Error(ErrorPrefixes.QueryParsingError, "Property count does not match value count.");

        return (moduleName, props, values);
    }
    
    private Result<(string ModuleGroupName, string TableName, Dictionary<string, string> UsingProperties, string[] Columns, string[] Values)> ParseInsertInto()
    {
        // INSERT INTO <moduleName>.<tableName> USING (<prop1>=<val1>, ...) (<columns>) VALUES (<values>)
        if (tokens.Length < 10)
            return new Error(ErrorPrefixes.QueryParsingError, "Invalid INSERT INTO syntax. Expected: INSERT INTO <module>.<table> USING (...) (...) VALUES (...)");

        return Result.Success()
            .Then(() => ParseTableNamePart(3))
            .Then(parsedTableName => ParseUsingClausePart(parsedTableName.NextIndex)
            .Then(parsedUsing => ParseColumnsPart(parsedUsing.NextIndex)
            .Then(parsedColumns => ParseValuesPart(parsedColumns.NextIndex)
            .Then<(string ModuleGroupName, string TableName, Dictionary<string, string> UsingProperties, string[] Columns, string[] Values)>(parsedValues =>
            {
                if (parsedColumns.Columns.Length != parsedValues.Values.Length)
                    return new Error(ErrorPrefixes.QueryParsingError, "Column count does not match value count.");

                return (
                    parsedTableName.ModuleGroupName,
                    parsedTableName.TableName,
                    parsedUsing.UsingProperties,
                    parsedColumns.Columns,
                    parsedValues.Values
                );
            }))));
    }

    private Result<(string ModuleGroupName, string TableName, int NextIndex)> ParseTableNamePart(int startIndex)
    {
        var fullTableName = tokens[startIndex -1].Split('.');
        if (fullTableName.Length != 2)
            return new Error(ErrorPrefixes.QueryParsingError, "Invalid table name format. Expected: <moduleName>.<tableName>");
        
        var moduleName = fullTableName[0];
        var tableName = fullTableName[1];

        return (moduleName, tableName, startIndex);
    }

    private Result<(Dictionary<string, string> UsingProperties, int NextIndex)> ParseUsingClausePart(int startIndex)
    {
        var usingIndex = TokenToKeywordIndex.Find(tokens, "USING", startIndex);
        if (usingIndex == -1)
            return new Error(ErrorPrefixes.QueryParsingError, "Missing USING clause in INSERT INTO.");
        
        var usingResult = ParseUsingClause(usingIndex + 1, out int usingPropsEnd);
        if (!usingResult.TryGetValue(out var usingProperties))
            return usingResult.Error;

        return (usingProperties, usingPropsEnd);
    }
    
    private Result<(string[] Columns, int NextIndex)> ParseColumnsPart(int startIndex)
    {
        var columnsResult = TokensToParenthesizedList.ParseParenthesizedList(tokens, startIndex, out int columnsEnd);
        if(!columnsResult.TryGetValue(out var columns))
            return columnsResult.Error;

        return (columns, columnsEnd);
    }

    private Result<(string[] Values, int NextIndex)> ParseValuesPart(int startIndex)
    {
        var valuesIndex = TokenToKeywordIndex.Find(tokens, "VALUES", startIndex);
        if (valuesIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing VALUES keyword.");

        var valuesResult = TokensToParenthesizedList.ParseParenthesizedList(tokens, valuesIndex + 1, out int valuesEnd);
        if(!valuesResult.TryGetValue(out var values))
            return valuesResult.Error;

        return (values, valuesEnd);
    }

    private Result<Dictionary<string, string>> ParseUsingClause(int startIndex, out int endIndex)
    {
        endIndex = -1;
        
        var nextParenIndex = Array.IndexOf(tokens, "(", startIndex);
        if (nextParenIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing columns list after USING clause.");
        
        endIndex = nextParenIndex - 1;

        if (endIndex < startIndex) return new Error(ErrorPrefixes.QueryParsingError, "Empty USING clause.");

        var usingClauseTokens = tokens[startIndex..nextParenIndex];
        
        var usingProperties = new Dictionary<string, string>();
        for (int i = 0; i < usingClauseTokens.Length; i += 3)
        {
            if (i + 2 >= usingClauseTokens.Length || usingClauseTokens[i+1] != "=") return new Error(ErrorPrefixes.QueryParsingError, "Malformed USING clause. Expected 'key=value' pairs.");
            
            var value = usingClauseTokens[i+2];
            if (value.StartsWith("'") && value.EndsWith("'"))
                value = value.Substring(1, value.Length - 2);

            usingProperties[usingClauseTokens[i]] = value;
        }
        
        return usingProperties;
    }

    private async Task<Result<Object[]>> AssertUsingClause((string ModuleGroupName, string TableName, Dictionary<string, string> UsingProperties, string[] Columns, string[] Values) parsed)
    {
        var cellEnvResult = await cellManager.GetEnvironment(parsed.ModuleGroupName);
        if (!cellEnvResult.TryGetValue(out var cellEnv)) return cellEnvResult.Error;

        var cellSchemaColumns = cellEnv.Columns;
        var usingIndexValues = new Dictionary<string, string>();

        foreach (var kvp in parsed.UsingProperties)
        {
            var col = cellSchemaColumns.FirstOrDefault(c => c.Name.Equals(kvp.Key, StringComparison.OrdinalIgnoreCase));
            if (col == null) return new Error(ErrorPrefixes.DataError, $"Property '{kvp.Key}' in USING clause does not exist in ModuleGroup '{parsed.ModuleGroupName}'.");
            if (!col.IsIndex) return new Error(ErrorPrefixes.DataError, $"Property '{kvp.Key}' in USING clause is not an indexed column for ModuleGroup '{parsed.ModuleGroupName}'.");

            usingIndexValues[col.Name] = kvp.Value;
        }

        if (usingIndexValues.Count != cellSchemaColumns.Count(c => c.IsIndex)) return new Error(ErrorPrefixes.DataError, $"USING clause requires values for all indexed ModuleGroup columns.");

        var cellInstanceHash = InstanceHasher.GenerateModuleInstanceHash(usingIndexValues);

        var getModuleInstanceResult = await cellManager.GetModuleInstance(parsed.ModuleGroupName, cellInstanceHash);
        if (getModuleInstanceResult.IsFailure) return getModuleInstanceResult.Error;
        return usingIndexValues.Values.Cast<object>().ToArray();
    }

    private Result<Object?[]> GetRowValues(Dictionary<string, string> rowValueMap, Column[] tableSchemaColumns, string tableName)
    {
        var tableRowValues = new object?[tableSchemaColumns.Length];
        var tablePkValues = new List<object>();

        for (int i = 0; i < tableSchemaColumns.Length; i++)
        {
            var col = tableSchemaColumns[i];
            if (rowValueMap.TryGetValue(col.Name, out var stringVal))
            {
                var parseValResult = TokensToDataParser.Parse(stringVal, col.DataType);
                if (!parseValResult.TryGetValue(out var typedVal)) return parseValResult.Error;

                var validationResult = DataValidator.Validate(typedVal, col);
                if (validationResult.IsFailure) return validationResult.Error;

                tableRowValues[i] = typedVal;
                if (col.IsPrimaryKey) tablePkValues.Add(typedVal);
            }
            else if (!col.IsNullable)
                return new Error(ErrorPrefixes.DataError, $"Column '{col.Name}' is not nullable and must be provided for table '{tableName}'.");
        }

        if (tableSchemaColumns.Any(c => c.IsPrimaryKey) && tablePkValues.Count == 0)
            return new Error(ErrorPrefixes.DataError, $"Table '{tableName}' has primary key(s), but no values were provided in the INSERT INTO statement.");
        
        return tableRowValues;
    }
}