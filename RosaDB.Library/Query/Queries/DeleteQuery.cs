using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Query.Queries;

public class DeleteQuery(
    string[] tokens,
    IModuleManager moduleManager,
    IDatabaseManager databaseManager,
    ILogReader logReader,
    ILogWriter logWriter,
    SessionState sessionState) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        // DELETE FROM <module>.<table> USING ... WHERE ...
        if (tokens[0].ToUpperInvariant() != "DELETE") return new Error(ErrorPrefixes.QueryParsingError, "Incorrect query type");

        var (_, fromIndex, whereIndex, usingIndex) = TokensToIndexesParser.ParseQueryTokens(tokens);
        var (module, tableName) = TokensToModuleAndTableParser.TokensToModuleAndName(tokens[fromIndex + 1]);
        var columns = sessionState.CurrentDatabase?.GetModule(module)?.GetTable(tableName)?.Columns;
        if (columns == null) return new DatabaseNotSetError();
        
        IAsyncEnumerable<Log> logs;
        if (usingIndex != -1)
        {
            var cellEnv = await databaseManager.GetModule(module);
            if (cellEnv.IsFailure) throw new InvalidOperationException(cellEnv.Error.Message);
                
            var result = await UsingClauseProcessor.Process(tokens, moduleManager, logReader, cellEnv.Value);
            if(result.IsFailure) throw new InvalidOperationException(result.Error.Message);
            logs = result.Value;
        }
        else logs = logReader.GetAllLogsForModuleTable(module, tableName);
            
        if (logs is null) return new Error(ErrorPrefixes.DataError, "No logs found");

        // 1. Get filtered stream
        var whereFunction = ConvertWHEREToFunction(whereIndex, usingIndex, columns.ToArray());
        var filteredStream = GetFilteredStream(logs, whereFunction, columns.ToArray());

        int count = 0;
        await foreach ((Row, Log) tuple in filteredStream)
        {
            logWriter.Delete(module, tableName, tuple.Item1.InstanceHash, tuple.Item2.Id);
            count++;
        }
        
        if (!sessionState.IsInTransaction) await logWriter.Commit();
        
        return new QueryResult("Delete was successful", count);
    }
    
    private async IAsyncEnumerable<(Row, Log)> GetFilteredStream(IAsyncEnumerable<Log> logs, Func<Row, bool> whereFunction, Column[] columns)
    {
        await foreach (var log in logs)
        {
            var rowResult = RowSerializer.Deserialize(log.TupleData, columns);
            if (rowResult.IsSuccess && whereFunction(rowResult.Value)) yield return (rowResult.Value, log);
        }
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