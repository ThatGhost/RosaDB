using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Query.Queries;

// TODO needs a complete rework
public class DeleteQuery(
    string[] tokens,
    IContextManager cellManager,
    ILogManager logManager,
    SessionState sessionState) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        // DELETE FROM <context>.<table> USING ... WHERE ...
        if (tokens[0].ToUpperInvariant() != "DELETE") return new Error(ErrorPrefixes.QueryParsingError, "Incorrect query type");

        var (_, fromIndex, whereIndex, usingIndex) = TokensToIndexesParser.ParseQueryTokens(tokens);
        var (contextName, tableName) = TokensToContextAndTableParser.TokensToContextAndName(tokens[fromIndex + 1]);

        var cellEnvResult = await cellManager.GetEnvironment(contextName);
        if (cellEnvResult.IsFailure) throw new InvalidOperationException(cellEnvResult.Error.Message);
        var cellEnv = cellEnvResult.Value;

        var columnsResult = await cellManager.GetColumnsFromTable(contextName, tableName);
        if (!columnsResult.TryGetValue(out var columns)) return columnsResult.Error;

        var contextInstancesResult = await cellManager.GetAllContextInstances(contextName);
        if (!contextInstancesResult.TryGetValue(out var allContextInstances)) return contextInstancesResult.Error;

        var instancesToProcess = allContextInstances as Row[] ?? allContextInstances.ToArray();
        IEnumerable<Row> contextInstancesToProcess = instancesToProcess;

        // Filter context instances if USING clause is present
        if (usingIndex != -1)
        {
            var endIndex = whereIndex != -1 ? whereIndex : tokens.Length;
            var usingTokens = tokens[(usingIndex + 1)..endIndex];
            var usingValues = new Dictionary<string, string>();
            for (int i = 0; i < usingTokens.Length; i += 4)
            {
                if (i + 2 < usingTokens.Length && usingTokens[i + 1] == "=")
                {
                    usingValues[usingTokens[i]] = usingTokens[i + 2];
                }
            }

            contextInstancesToProcess = instancesToProcess.Where(instance => {
                foreach (var usingValue in usingValues)
                {
                    var colIndex = Array.FindIndex(instance.Columns, c => c.Name.Equals(usingValue.Key, StringComparison.OrdinalIgnoreCase));
                    if (colIndex == -1) return false;

                    var instanceValue = instance.Values[colIndex]?.ToString();
                    if (instanceValue == null || !instanceValue.Equals(usingValue.Value, StringComparison.OrdinalIgnoreCase))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        int count = 0;
        var whereFunction = ConvertWHEREToFunction(whereIndex, usingIndex, columns);

        foreach (var instance in contextInstancesToProcess)
        {
            var contextIndexValues = cellEnv.GetIndexValues(instance);
            var logsForInstance = logManager.GetAllLogsForContextInstanceTable(contextName, tableName, contextIndexValues);

            await foreach (var log in logsForInstance)
            {
                var rowResult = RowSerializer.Deserialize(log.TupleData, columns);
                if (rowResult.IsSuccess && whereFunction(rowResult.Value))
                {
                    logManager.Delete(contextName, tableName, contextIndexValues, log.Id);
                    count++;
                }
            }
        }

        if (!sessionState.IsInTransaction) await logManager.Commit();
        
        return new QueryResult("Delete was successful", count);
    }
    
    private Func<Row, bool> ConvertWHEREToFunction(int whereIndex, int usingIndex, Column[] allColumns)
    {
        if (whereIndex == -1) return _ => true;

        var conditions = new List<(int columnIndex, string op, object parsedValue)>();
        
        int whereClauseEnd = (usingIndex != -1 && usingIndex > whereIndex) ? usingIndex : tokens.Length;
        var whereTokens = tokens[(whereIndex + 1)..whereClauseEnd];

        for (int i = 0; i < whereTokens.Length; i += 4)
        {
            if (i + 2 >= whereTokens.Length) break;

            var columnName = whereTokens[i];
            var op = whereTokens[i + 1];
            var stringValue = whereTokens[i + 2];

            var columnIndex = Array.FindIndex(allColumns, c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (columnIndex == -1) return _ => false;

            var columnDataType = allColumns[columnIndex].DataType;
            var parseResult = TokensToDataParser.Parse(stringValue, columnDataType);
            if (parseResult.IsFailure) return _ => false;

            conditions.Add((columnIndex, op, parseResult.Value));

            if (i + 3 >= whereTokens.Length || !whereTokens[i + 3].Equals("AND", StringComparison.OrdinalIgnoreCase)) break;
        }

        if (conditions.Count == 0) return _ => true;

        return row =>
        {
            foreach (var (columnIndex, op, parsedValue) in conditions)
            {
                if (columnIndex >= row.Values.Length) return false;
                var rowValue = row.Values[columnIndex];
                if (rowValue == null) return false;

                return op switch
                {
                    "=" => DataComparer.CompareEquals(rowValue, parsedValue),
                    ">" => DataComparer.CompareGreaterThan(rowValue, parsedValue),
                    "<" => DataComparer.CompareLessThan(rowValue, parsedValue),
                    ">=" => DataComparer.CompareGreaterThanOrEqual(rowValue, parsedValue),
                    "<=" => DataComparer.CompareLessThanOrEqual(rowValue, parsedValue),
                    _ => false
                };
            }
            return true;
        };
    }
}