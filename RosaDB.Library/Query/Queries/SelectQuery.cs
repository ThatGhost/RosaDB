using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using RosaDB.Library.Validation;

namespace RosaDB.Library.Query.Queries
{
    public class SelectQuery(string[] tokens, ILogManager logManager, ICellManager cellManager) : IQuery
    {
        public async ValueTask<QueryResult> Execute()
        {
            var (selectIndex, fromIndex, whereIndex, usingIndex) = ParseQueryTokens();
            
            var tableNameParts = tokens[fromIndex + 1].Split('.');
            var cellName = tableNameParts[0];
            var tableName = tableNameParts[1];

            var columnsResult = await cellManager.GetColumnsFromTable(cellName, tableName);
            if (!columnsResult.TryGetValue(out var columns)) return columnsResult.Error;

            return new QueryResult(StreamRows(selectIndex, fromIndex, whereIndex, usingIndex, columns));
        }

        private async IAsyncEnumerable<Row> StreamRows(int selectIndex, int fromIndex, int whereIndex, int usingIndex, Column[] columns)
        {
            var tableNameParts = tokens[fromIndex + 1].Split('.');
            var cellName = tableNameParts[0];
            var tableName = tableNameParts[1];

            IAsyncEnumerable<Log> logs;
            if (usingIndex != -1)
            {
                var cellEnv = await cellManager.GetEnvironment(cellName);
                if (cellEnv.IsFailure) throw new InvalidOperationException(cellEnv.Error.Message);
                
                var result = await ApplyUsing(tableName, cellName, whereIndex, usingIndex, cellEnv.Value);
                if(result.IsFailure) throw new InvalidOperationException(result.Error.Message);
                logs = result.Value;
            }
            else logs = logManager.GetAllLogsForCellTable(cellName, tableName);
            
            if (logs is null) yield break;

            // 1. Get filtered stream
            var whereFunction = ConvertWHEREToFunction(whereIndex, usingIndex, columns);
            var filteredStream = GetFilteredStream(logs, whereFunction, columns);

            // 2. Apply projection if needed
            var projectionTokens = tokens[(selectIndex + 1)..fromIndex];
            bool hasProjection = projectionTokens.Length > 0 && projectionTokens[0] != "*";

            IAsyncEnumerable<Row> finalStream = hasProjection 
                ? ApplyProjection(filteredStream, projectionTokens, columns) 
                : filteredStream;

            // 3. Yield from the final stream
            await foreach (var row in finalStream)
            {
                yield return row;
            }
        }

        private async IAsyncEnumerable<Row> GetFilteredStream(IAsyncEnumerable<Log> logs, Func<Row, bool> whereFunction, Column[] columns)
        {
            await foreach (var log in logs)
            {
                var rowResult = RowSerializer.Deserialize(log.TupleData, columns);
                if (rowResult.IsSuccess && whereFunction(rowResult.Value))
                {
                    yield return rowResult.Value;
                }
            }
        }

        private (int selectIndex, int fromIndex, int whereIndex, int usingIndex) ParseQueryTokens()
        {
            int selectIdx = -1;
            int fromIdx = -1;
            int whereIdx = -1;
            int usingIdx = -1;

            for (int i = 0; i < tokens.Length; i++)
            {
                if (tokens[i].Equals("SELECT", StringComparison.OrdinalIgnoreCase)) selectIdx = i;
                else if (tokens[i].Equals("FROM", StringComparison.OrdinalIgnoreCase)) fromIdx = i;
                else if (tokens[i].Equals("WHERE", StringComparison.OrdinalIgnoreCase)) whereIdx = i;
                else if (tokens[i].Equals("USING", StringComparison.OrdinalIgnoreCase)) usingIdx = i;
            }
            return (selectIdx, fromIdx, whereIdx, usingIdx);
        }

        private async Task<Result<IAsyncEnumerable<Log>>> ApplyUsing(string tableName, string cellName, int whereIndex, int usingIndex, CellEnvironment cellEnv)
        {
            var endIndex = whereIndex != -1 ? whereIndex : tokens.Length - 1;
            var usingTokens = tokens[(usingIndex + 1)..endIndex];

            var usingValues = new Dictionary<string, (string value, string operation)>();
            for (int i = 0; i < usingTokens.Length; i += 4) usingValues[usingTokens[i]] = new(usingTokens[i + 2], usingTokens[i + 1]);
            
            // check if all and only index columns are present for cell then use the cell instance
            var indexStringValues = usingValues.Keys.Where(u => cellEnv.IndexColumns.Select(i => i.Name).Contains(u)).ToArray();
            if (usingValues.Count == cellEnv.IndexColumns.Length && indexStringValues.Length == cellEnv.IndexColumns.Length)
            {   
                var indexValues = new List<object>();
                foreach (var cellEnvIndexColumn in cellEnv.IndexColumns)
                {
                    var parseResult = StringToDataParser.Parse(usingValues[cellEnvIndexColumn.Name].value, cellEnvIndexColumn.DataType);
                    if (parseResult.IsFailure) return parseResult.Error;
                    indexValues.Add(parseResult.Value);
                }
                
                return Result<IAsyncEnumerable<Log>>.Success(logManager.GetAllLogsForCellInstanceTable(cellName, tableName, indexValues.ToArray()));
            }
            
            // if it's not the indexes then get all the cell instances and concat all the conforming cells
            var cellsResult = await cellManager.GetAllCellInstances(cellName);
            if (cellsResult.IsFailure) return cellsResult.Error;

            List<Row> cellsThatApply = []; 
            foreach (Row cell in cellsResult.Value)
            {
                bool doesUsingApply = true;
                foreach (var usingValue in usingValues)
                {
                    var columnIndex = Array.FindIndex(cell.Columns, c => c.Name.Equals(usingValue.Key, StringComparison.OrdinalIgnoreCase));
                    if (columnIndex == -1) { doesUsingApply = false; break; }

                    var rowValue = cell.Values[columnIndex];
                    if(rowValue == null) { doesUsingApply = false; break; }
                    
                    var parsedValueResult = StringToDataParser.Parse(usingValue.Value.value, cell.Columns[columnIndex].DataType);
                    if(parsedValueResult.IsFailure) { doesUsingApply = false; break; }

                    if (usingValue.Value.operation == "=") {
                        if (!rowValue.Equals(parsedValueResult.Value)) { doesUsingApply = false; break; }
                    } else {
                        doesUsingApply = false; break;
                    }
                }
                if(doesUsingApply) cellsThatApply.Add(cell);
            }

            return Result<IAsyncEnumerable<Log>>.Success(TurnCellRowsToLogs(cellsThatApply, tableName, cellName, cellEnv));
        }

        private async IAsyncEnumerable<Log> TurnCellRowsToLogs(List<Row> cells, string tableName, string cellName, CellEnvironment cellEnv)
        {
            foreach (var cell in cells)
            {
                await foreach (var log in logManager.GetAllLogsForCellInstanceTable(cellName, tableName, cellEnv.GetIndexValues(cell)))
                {
                    yield return log;
                }
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
                var parseResult = StringToDataParser.Parse(stringValue, columnDataType);
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

                    if (op == "=")
                    {
                        if (!rowValue.Equals(parsedValue)) return false;
                    }
                    else return false;
                }
                // All conditions passed
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