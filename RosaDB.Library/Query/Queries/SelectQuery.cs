using RosaDB.Library.Core;
using RosaDB.Library.Models;
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

            IAsyncEnumerable<Log> logs;
            if (usingIndex != -1)
            {
                var cellEnv = await cellManager.GetEnvironment(cellName);
                if (cellEnv.IsFailure) return cellEnv.Error;
                
                var endIndex = whereIndex != -1 ? whereIndex : tokens.Length - 1;
                var usingTokens = tokens[(usingIndex + 1)..endIndex];

                var usingValues = new Dictionary<string, string>();
                for (int i = 0; i < usingTokens.Length; i += 4)
                {
                    usingValues[usingTokens[i]] = usingTokens[i + 2];
                }

                object[] orderedValues = cellEnv.Value.Columns
                    .Where(c => c.IsIndex)
                    .Select(c => StringToDataParser.Parse(usingValues[c.Name], c.DataType).Value)
                    .Where(v => v != null)
                    .ToArray()!;

                if (orderedValues.Length == 0)
                    return new Error(ErrorPrefixes.QueryParsingError, "No values found in the USING statement");
                
                logs = logManager.GetAllLogsForCellInstanceTable(cellName, tableName, orderedValues);
            }
            else
            {
                logs = logManager.GetAllLogsForCellTable(cellName, tableName);
            }
            
            var columnsResult = await cellManager.GetColumnsFromTable(cellName, tableName);
            if (!columnsResult.TryGetValue(out var columns)) return columnsResult.Error;
            
            var rows = new List<Row>();
            await foreach (var log in logs)
            {
                var row = RowSerializer.Deserialize(log.TupleData, columns);
                if (row.IsSuccess)
                {
                    rows.Add(row.Value);
                }
            }
            
            if (whereIndex != -1)
            {
                var whereTokens = tokens[(whereIndex + 1)..^1];
                for (int i = 0; i < whereTokens.Length; i += 4)
                {
                    var columnName = whereTokens[i];
                    var op = whereTokens[i + 1];
                    var value = whereTokens[i + 2];
                    rows = rows.Where(r => ApplyWhere(r, columnName, op, value)).ToList();
                }
            }

            var projectionIndex = FindKeywordIndex("SELECT");
            var from = FindKeywordIndex("FROM");
            var projectionTokens = tokens[(projectionIndex + 1)..from];

            if (projectionTokens.Length > 0 && projectionTokens[0] != "*")
            {
                var projectedRows = new List<Row>();
                foreach (var row in rows)
                {
                    var projectedRow = ApplyProjection(row, projectionTokens);
                    if (projectedRow.IsSuccess)
                    {
                        projectedRows.Add(projectedRow.Value);
                    }
                    else
                    {
                        return projectedRow.Error;
                    }
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
                if (colName == ",")
                    continue;
                
                var colIndex = Array.FindIndex(row.Columns, c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
                if (colIndex != -1)
                {
                    newColumns.Add(row.Columns[colIndex]);
                    var value = row.Values[colIndex];
                    if (value != null)
                    {
                        newValues.Add(value);
                    }
                }
            }

            return Row.Create(newValues.ToArray(), newColumns.ToArray());
        }

        private bool ApplyWhere(Row row, string columnName, string op, string value)
        {
            var columnIndex = Array.FindIndex(row.Columns, c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (columnIndex == -1)
            {
                return false;
            }

            var rowValue = row.Values[columnIndex];
            if(rowValue == null)
                return false;
            
            var parsedValue = StringToDataParser.Parse(value, row.Columns[columnIndex].DataType).Value;

            if (op == "=")
            {
                return rowValue.Equals(parsedValue);
            }

            return false;
        }

        private int FindKeywordIndex(string keyword, int startIndex = 0)
        {
            for (int i = startIndex; i < tokens.Length; i++)
                if (tokens[i].Equals(keyword, StringComparison.OrdinalIgnoreCase)) return i;
            return -1;
        }
    }
}