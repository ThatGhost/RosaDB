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
            var fromIndex = FindKeywordIndex("FROM");
            var tableNameParts = tokens[fromIndex + 1].Split('.');
            var cellName = tableNameParts[0];
            var tableName = tableNameParts[1];
            var whereIndex = FindKeywordIndex("WHERE");
            var usingIndex = FindKeywordIndex("USING");
            var projectionIndex = FindKeywordIndex("SELECT");
            var from = FindKeywordIndex("FROM");

            IAsyncEnumerable<Log>? logs = null;
            if (usingIndex != -1)
            {
                var cellEnv = await cellManager.GetEnvironment(cellName);
                if (cellEnv.IsFailure) return cellEnv.Error;
                
                var result = await ApplyUsing(tableName, cellName, whereIndex, usingIndex, cellEnv.Value);
                if(result.IsFailure) return result.Error;
                logs = result.Value;
            }
            else logs = logManager.GetAllLogsForCellTable(cellName, tableName);
            if (logs is null) return new QueryResult(new CriticalError());
            
            var columnsResult = await cellManager.GetColumnsFromTable(cellName, tableName);
            if (!columnsResult.TryGetValue(out var columns)) return columnsResult.Error;

            Func<Row, bool> WhereFunction = ConvertWHEREToFunction(whereIndex);
            
            var rows = new List<Row>();
            await foreach (var log in logs)
            {
                var row = RowSerializer.Deserialize(log.TupleData, columns);
                if (row.IsSuccess && WhereFunction(row.Value))
                {
                    rows.Add(row.Value);
                }
            }
            
            var projectionTokens = tokens[(projectionIndex + 1)..from];
            if (projectionTokens.Length > 0 && projectionTokens[0] != "*")
            {
                var projectedRows = new List<Row>();
                foreach (var row in rows)
                {
                    var projectedRow = ApplyProjection(row, projectionTokens);
                    if (projectedRow.IsSuccess) projectedRows.Add(projectedRow.Value);
                    else return projectedRow.Error;
                }
                return new QueryResult(projectedRows);
            }
            
            return new QueryResult(rows);
        }

        private Result<Row> ApplyProjection(Row row, string[] projection)
        {
            var newColumns = new List<Column>();
            var newValues = new List<object>();

            foreach (var colName in projection)
            {
                if (colName == ",") continue;
                
                var colIndex = Array.FindIndex(row.Columns, c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
                if (colIndex != -1)
                {
                    newColumns.Add(row.Columns[colIndex]);
                    var value = row.Values[colIndex];
                    if (value != null) newValues.Add(value);
                }
            }

            return Row.Create(newValues.ToArray(), newColumns.ToArray());
        }

        private bool ApplyWhere(Row row, string columnName, string op, string value)
        {
            var columnIndex = Array.FindIndex(row.Columns, c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (columnIndex == -1) return false;

            var rowValue = row.Values[columnIndex];
            if(rowValue == null) return false;
            
            var parsedValue = StringToDataParser.Parse(value, row.Columns[columnIndex].DataType).Value;

            if (op == "=") return rowValue.Equals(parsedValue);
            //TODO
            //if (op == ">") return rowValue > parsedValue;
            //if (op == ">=") return rowValue > parsedValue;
            //if (op == "<") return rowValue > parsedValue;
            //if (op == "<=") return rowValue > parsedValue;
            return false;
        }

        private int FindKeywordIndex(string keyword, int startIndex = 0)
        {
            for (int i = startIndex; i < tokens.Length; i++)
                if (tokens[i].Equals(keyword, StringComparison.OrdinalIgnoreCase)) return i;
            return -1;
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
                foreach (var usingValue in usingValues) if(!ApplyWhere(cell, usingValue.Key, usingValue.Value.operation, usingValue.Value.value)) { 
                    doesUsingApply = false;
                    break;
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

        private Func<Row, bool> ConvertWHEREToFunction(int whereIndex)
        {
            if (whereIndex == -1) return _ => true;
            var whereTokens = tokens[(whereIndex + 1)..^1];
            for (int i = 0; i < whereTokens.Length; i += 4)
            {
                var columnName = whereTokens[i];
                var op = whereTokens[i + 1];
                if (i + 2 < 0 || i + 2 >= whereTokens.Length) continue;
                
                var value = whereTokens[i + 2];
                return row => ApplyWhere(row, columnName, op, value);
            }
            return _ => true;
        }
    }
}