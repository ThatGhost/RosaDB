using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.Security.Cryptography;
using System.Text;

namespace RosaDB.Library.Query.Queries
{
    public class SelectQuery(string[] tokens, LogManager logManager, ICellManager cellManager, IIndexManager indexManager) : IQuery
    {
        public async Task<QueryResult> Execute()
        {
            // Basic parsing: SELECT <columns> FROM <tableName> IN <cellName> WHERE <column> = <value>
            if (tokens.Length < 6 || tokens[0].ToUpperInvariant() != "SELECT" || tokens[2].ToUpperInvariant() != "FROM" || tokens[4].ToUpperInvariant() != "IN")
            {
                return new Error(ErrorPrefixes.QueryParsingError, "Invalid SELECT syntax. Expected: SELECT <cols> FROM <tableName> IN <cellName> [WHERE ...]");
            }

            List<string> selectedColumns = tokens[1] == "*" ? new List<string> { "*" } : tokens[1].Split(',').Select(s => s.Trim()).ToList();
            string tableName = tokens[3];
            string cellName = tokens[5];

            // Where clause parsing
            string? filterColumn = null;
            string? filterValue = null;
            if (tokens.Length > 7 && tokens[6].ToUpperInvariant() == "WHERE")
            {
                // Extremely basic parser for "column = value"
                var whereTokens = tokens.Skip(7).ToList();
                var equalsIndex = whereTokens.IndexOf("=");
                if (equalsIndex > 0 && equalsIndex < whereTokens.Count - 1)
                {
                    filterColumn = whereTokens[equalsIndex - 1];
                    filterValue = whereTokens[equalsIndex + 1].Trim('\'', '"'); 
                }
            }

            var columnsResult = await cellManager.GetColumnsFromTable(cellName, tableName);
            if (!columnsResult.TryGetValue(out var schemaColumns)) return columnsResult.Error;

            var logs = new List<Log>();
            Column? indexColumn = null;

            if (filterColumn != null && filterValue != null)
            {
                indexColumn = schemaColumns.FirstOrDefault(c => c.Name.Equals(filterColumn, StringComparison.OrdinalIgnoreCase));
                if (indexColumn != null && (indexColumn.IsIndex || indexColumn.IsPrimaryKey)) // Search using index
                {
                    var key = IndexKeyConverter.ToByteArray(filterValue);
                    TableInstanceIdentifier identifier;

                    if (indexColumn.IsPrimaryKey)
                    {
                        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(filterValue)));
                        identifier = new TableInstanceIdentifier(cellName, tableName, hash);
                    }
                    else // Is a secondary index
                    {
                        identifier = new TableInstanceIdentifier(cellName, tableName, "_TABLE_");
                    }
                    
                    var searchResult = indexManager.Search(identifier, indexColumn.Name.Trim(), key);

                    if (searchResult.TryGetValue(out var logLocation))
                    {
                        var logResult = await logManager.GetLogAtLocation(logLocation);
                        if (logResult.IsSuccess)
                        {
                            logs.Add(logResult.Value);
                        }
                    }
                }
                else
                {
                    // Fallback to table scan if no index
                    await foreach (var log in logManager.GetAllLogsForCellTable(cellName, tableName))
                    {
                        logs.Add(log);
                    }
                }
            }
            else // No WHERE clause
            {
                await foreach (var log in logManager.GetAllLogsForCellTable(cellName, tableName))
                {
                    logs.Add(log);
                }
            }
            
            var finalRows = new List<Row>();
            var seenLogIds = new HashSet<long>();

            foreach (var log in logs.OrderByDescending(l => l.Date))
            {
                if (!seenLogIds.Add(log.Id)) continue;
                if (log.IsDeleted) continue;

                var rowResult = RowSerializer.Deserialize(log.TupleData, schemaColumns);
                if (!rowResult.TryGetValue(out var row)) continue;
                
                if (filterColumn != null && filterValue != null && !(indexColumn != null && (indexColumn.IsIndex || indexColumn.IsPrimaryKey)))
                {
                    var colIndex = Array.FindIndex(row.Columns, c => c.Name.Equals(filterColumn, StringComparison.OrdinalIgnoreCase));
                    if (colIndex == -1 || row.Values[colIndex]?.ToString() != filterValue)
                    {
                        continue; // Doesn't match filter
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
    }
}
