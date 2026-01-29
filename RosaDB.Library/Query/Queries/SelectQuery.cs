using RosaDB.Library.Models;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Query.Queries
{
    public class SelectQuery(string[] tokens, ILogReader logReader, IModuleManager cellManager) : IQuery
    {
        public async ValueTask<QueryResult> Execute()
        {
            var (selectIndex, fromIndex, whereIndex, usingIndex) = TokensToIndexesParser.ParseQueryTokens(tokens);
            var (module, tableName) = TokensToModuleAndTableParser.TokensToModuleAndName(tokens[fromIndex + 1]);

            var columnsResult = await cellManager.GetColumnsFromTable(module, tableName);
            if (!columnsResult.TryGetValue(out var columns)) return columnsResult.Error;

            return new QueryResult(StreamRows(selectIndex, fromIndex, whereIndex, usingIndex, columns));
        }

        private async IAsyncEnumerable<Row> StreamRows(int selectIndex, int fromIndex, int whereIndex, int usingIndex, Column[] columns)
        {
            var (module, tableName) = TokensToModuleAndTableParser.TokensToModuleAndName(tokens[fromIndex + 1]);

            IAsyncEnumerable<Log> logs;
            if (usingIndex != -1)
            {
                var cellEnv = await cellManager.GetEnvironment(module);
                if (cellEnv.IsFailure) throw new InvalidOperationException(cellEnv.Error.Message);
                
                var result = await UsingClauseProcessor.Process(tokens, cellManager, logReader, cellEnv.Value);
                if(result.IsFailure) throw new InvalidOperationException(result.Error.Message);
                logs = result.Value;
            }
            else logs = logReader.GetAllLogsForModuleTable(module, tableName);
            
            if (logs is null) yield break;

            // 1. Get filtered stream
            var whereFunction = ConvertWHEREToFunction(whereIndex, usingIndex, columns);
            var filteredStream = GetFilteredStream(logs, whereFunction, columns);

            // 2. Apply projection if needed
            var projectionTokens = tokens[(selectIndex + 1)..fromIndex];
            bool hasProjection = projectionTokens.Length > 0 && projectionTokens[0] != "*";

            IAsyncEnumerable<Row> finalStream = hasProjection ? ApplyProjection(filteredStream, projectionTokens, columns) : filteredStream;

            // 3. Yield from the final stream
            await foreach (var row in finalStream) yield return row;
        }

        private async IAsyncEnumerable<Row> GetFilteredStream(IAsyncEnumerable<Log> logs, Func<Row, bool> whereFunction, Column[] columns)
        {
            await foreach (var log in logs)
            {
                var rowResult = RowSerializer.Deserialize(log.TupleData, columns);
                if (rowResult.IsSuccess && whereFunction(rowResult.Value)) yield return rowResult.Value;
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
        
        private async IAsyncEnumerable<Row> ApplyProjection(IAsyncEnumerable<Row> rows, string[] projection, Column[] originalColumns)
        {
            var projectedColumns = new List<Column>();
            var projectedIndices = new List<int>();

            foreach (var colName in projection)
            {
                if (colName == ",") continue;
                
                var colIndex = Array.FindIndex(originalColumns, c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
                if (colIndex != -1)
                {
                    projectedColumns.Add(originalColumns[colIndex]);
                    projectedIndices.Add(colIndex);
                }
            }
            var finalColumns = projectedColumns.ToArray();

            await foreach (var row in rows)
            {
                var newValues = new object?[projectedIndices.Count];
                for(int i = 0; i < projectedIndices.Count; i++)
                {
                    newValues[i] = row.Values[projectedIndices[i]];
                }

                var createResult = Row.Create(newValues, finalColumns);
                if (createResult.IsSuccess) yield return createResult.Value;
            }
        }
    }
}