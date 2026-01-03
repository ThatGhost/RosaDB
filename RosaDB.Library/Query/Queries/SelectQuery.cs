using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Query.Queries
{
    public class SelectQuery(string[] tokens, ILogManager logManager, ICellManager cellManager) : IQuery
    {
        public async ValueTask<QueryResult> Execute()
        {
            if (tokens.Length < 4 || tokens[0].ToUpperInvariant() != "SELECT")
                return new Error(ErrorPrefixes.QueryParsingError, "Invalid SELECT syntax.");

            // 1. Parse Columns
            var fromIndex = FindKeywordIndex("FROM");
            if (fromIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing FROM clause.");

            List<string> selectedColumns = tokens[1] == "*" 
                ? new List<string> { "*" } 
                : string.Join("", tokens[1..fromIndex]).Split(',').Select(s => s.Trim()).ToList();

            // 2. Parse Table
            if (fromIndex + 1 >= tokens.Length) return new Error(ErrorPrefixes.QueryParsingError, "Missing table name after FROM.");
            var tableToken = tokens[fromIndex + 1];
            var tableParts = tableToken.Split('.');
            if (tableParts.Length != 2) return new Error(ErrorPrefixes.QueryParsingError, "Invalid table format. Expected <CellGroup>.<TableName>");
            
            var cellGroupName = tableParts[0];
            var tableName = tableParts[1];

            // 3. Parse optional clauses
            int currentIndex = fromIndex + 2;
            
            Dictionary<string, string>? usingProperties = null;
            string? whereCol = null;
            string? whereVal = null;

            while (currentIndex < tokens.Length)
            {
                var token = tokens[currentIndex].ToUpperInvariant();
                if (token == "USING")
                {
                    // Parse USING
                    usingProperties = new Dictionary<string, string>();
                    currentIndex++;
                    while (currentIndex < tokens.Length)
                    {
                        if (tokens[currentIndex].ToUpperInvariant() == "WHERE") break;
                        
                        // Expect key = value
                        if (currentIndex + 2 >= tokens.Length) return new Error(ErrorPrefixes.QueryParsingError, "Malformed USING clause.");
                        if (tokens[currentIndex + 1] != "=") return new Error(ErrorPrefixes.QueryParsingError, "Expected '=' in USING clause.");
                        
                        var key = tokens[currentIndex];
                        var val = tokens[currentIndex + 2].Trim('"');
                        usingProperties[key] = val;
                        
                        currentIndex += 3;
                        if (currentIndex < tokens.Length && tokens[currentIndex] == ",") currentIndex++;
                    }
                }
                else if (token == "WHERE")
                {
                    // Parse WHERE (Basic: col = val)
                    if (currentIndex + 3 >= tokens.Length) return new Error(ErrorPrefixes.QueryParsingError, "Malformed WHERE clause.");
                    if (tokens[currentIndex + 2] != "=") return new Error(ErrorPrefixes.QueryParsingError, "Expected '=' in WHERE clause.");
                    
                    whereCol = tokens[currentIndex + 1];
                    whereVal = tokens[currentIndex + 3].Trim('"');
                    
                    currentIndex += 4; 
                }
                else
                {
                     return new Error(ErrorPrefixes.QueryParsingError, $"Unexpected token: {tokens[currentIndex]}");
                }
            }

            // 4. Execution Prep
            var columnsResult = await cellManager.GetColumnsFromTable(cellGroupName, tableName);
            if (!columnsResult.TryGetValue(out var schemaColumns)) return columnsResult.Error;

            // 5. Determine Instance Index Values (if USING)
            object[]? instanceIndex = null;
            if (usingProperties != null)
            {
                 var cellEnvResult = await cellManager.GetEnvironment(cellGroupName);
                 if (!cellEnvResult.TryGetValue(out var cellEnv)) return cellEnvResult.Error;

                 var usingIndexValues = new Dictionary<string, string>();
                 foreach (var kvp in usingProperties)
                 {
                    var col = cellEnv.Columns.FirstOrDefault(c => c.Name.Equals(kvp.Key, StringComparison.OrdinalIgnoreCase));
                    if (col == null) return new Error(ErrorPrefixes.DataError, $"Property '{kvp.Key}' in USING clause does not exist.");
                    if (!col.IsIndex) return new Error(ErrorPrefixes.DataError, $"Property '{kvp.Key}' is not an index.");
                    usingIndexValues[col.Name] = kvp.Value;
                 }
                 
                 if (usingIndexValues.Count != cellEnv.Columns.Count(c => c.IsIndex)) 
                     return new Error(ErrorPrefixes.DataError, "USING clause must provide all indexed properties.");

                 instanceIndex = usingIndexValues.Values.Cast<object>().ToArray();
            }

            // 6. Fetch Logs
            var logs = new List<Log>();

            IAsyncEnumerable<Log> logSource;
            if (instanceIndex != null)
            {
                logSource = logManager.GetAllLogsForCellInstanceTable(cellGroupName, tableName, instanceIndex);
            }
            else
            {
                logSource = logManager.GetAllLogsForCellTable(cellGroupName, tableName);
            }
            
            await foreach(var log in logSource) logs.Add(log);

            var finalRows = new List<Row>();
            var seenLogIds = new HashSet<long>();

            foreach (var log in logs.OrderByDescending(l => l.Date))
            {
                if (!seenLogIds.Add(log.Id)) continue;
                if (log.IsDeleted) continue;

                var rowResult = RowSerializer.Deserialize(log.TupleData, schemaColumns);
                if (!rowResult.TryGetValue(out var row)) continue;
                
                if (whereCol != null && whereVal != null)
                {
                    var colIndex = Array.FindIndex(row.Columns, c => c.Name.Equals(whereCol, StringComparison.OrdinalIgnoreCase));
                    if (colIndex == -1 || row.Values[colIndex]?.ToString() != whereVal)
                    {
                        continue; 
                    }
                }
                
                finalRows.Add(row);
            }
            
            // Projection
            var projectedRows = new List<Row>();
            if (selectedColumns.Count == 1 && selectedColumns[0] == "*")
            {
                projectedRows.AddRange(finalRows);
            }
            else
            {
                foreach(var row in finalRows)
                {
                    var projectedValues = new List<object?>();
                    var projectedColumns = new List<Column>();
                    foreach (var selColName in selectedColumns)
                    {
                        var colIndex = Array.FindIndex(row.Columns, c => c.Name.Equals(selColName, StringComparison.OrdinalIgnoreCase));
                        if (colIndex != -1)
                        {
                            projectedColumns.Add(row.Columns[colIndex]);
                            projectedValues.Add(row.Values[colIndex]);
                        }
                    }
                    var newRowResult = Row.Create(projectedValues.ToArray(), projectedColumns.ToArray());
                    if(newRowResult.IsSuccess)
                    {
                        projectedRows.Add(newRowResult.Value);
                    }
                }
            }

            return new QueryResult(projectedRows);
        }

        private int FindKeywordIndex(string keyword, int startIndex = 0)
        {
            for (int i = startIndex; i < tokens.Length; i++)
                if (tokens[i].Equals(keyword, StringComparison.OrdinalIgnoreCase)) return i;
            return -1;
        }
    }
}