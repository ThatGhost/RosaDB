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

            var usingIndex = FindKeywordIndex("USING");

            IAsyncEnumerable<Log> logs;
            if (usingIndex != -1)
            {
                var cellEnv = await cellManager.GetEnvironment(cellName);
                if (cellEnv.IsFailure) return cellEnv.Error;
                
                var whereIndex = FindKeywordIndex("WHERE");
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

            return new QueryResult(rows);
        }

        private int FindKeywordIndex(string keyword, int startIndex = 0)
        {
            for (int i = startIndex; i < tokens.Length; i++)
                if (tokens[i].Equals(keyword, StringComparison.OrdinalIgnoreCase)) return i;
            return -1;
        }
    }
}