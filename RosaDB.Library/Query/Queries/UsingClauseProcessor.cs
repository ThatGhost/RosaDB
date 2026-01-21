using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries
{
    public static class UsingClauseProcessor
    {
        public static async Task<Result<IAsyncEnumerable<Log>>> Process(
            string[] tokens,
            IContextManager cellManager,
            ILogManager logManager,
            ContextEnvironment cellEnv)
        {
            var (_, fromIndex, whereIndex, usingIndex) = TokensToIndexesParser.ParseQueryTokens(tokens);
            var (contextName, tableName) = TokensToContextAndTableParser.TokensToContextAndName(tokens[fromIndex + 1]);
            
            var endIndex = whereIndex != -1 ? whereIndex : tokens.Length - 1;
            var usingTokens = tokens[(usingIndex + 1)..endIndex];

            var usingValues = new Dictionary<string, (string value, string operation)>();
            for (int i = 0; i < usingTokens.Length; i += 4) usingValues[usingTokens[i]] = new(usingTokens[i + 2], usingTokens[i + 1]);

            // check if all and only index columns are present for context then use the context instance
            var indexStringValues = usingValues.Keys.Where(u => cellEnv.IndexColumns.Select(i => i.Name).Contains(u)).ToArray();
            if (usingValues.Count == cellEnv.IndexColumns.Length && indexStringValues.Length == cellEnv.IndexColumns.Length)
            {
                var indexValues = new List<object>();
                foreach (var cellEnvIndexColumn in cellEnv.IndexColumns)
                {
                    var parseResult = TokensToDataParser.Parse(usingValues[cellEnvIndexColumn.Name].value, cellEnvIndexColumn.DataType);
                    if (parseResult.IsFailure) return parseResult.Error;
                    indexValues.Add(parseResult.Value);
                }

                return Result<IAsyncEnumerable<Log>>.Success(logManager.GetAllLogsForContextInstanceTable(contextName, tableName, indexValues.ToArray()));
            }

            // if it's not the indexes then get all the context instances and concat all the conforming cells
            var cellsResult = await cellManager.GetAllContextInstances(contextName);
            if (cellsResult.IsFailure) return cellsResult.Error;

            List<Row> cellsThatApply = [];
            foreach (Row context in cellsResult.Value)
            {
                bool doesUsingApply = true;
                foreach (var usingValue in usingValues)
                {
                    var columnIndex = Array.FindIndex(context.Columns, c => c.Name.Equals(usingValue.Key, StringComparison.OrdinalIgnoreCase));
                    if (columnIndex == -1) { doesUsingApply = false; break; }

                    var rowValue = context.Values[columnIndex];
                    if (rowValue == null) { doesUsingApply = false; break; }

                    var parsedValueResult = TokensToDataParser.Parse(usingValue.Value.value, context.Columns[columnIndex].DataType);
                    if (parsedValueResult.IsFailure) { doesUsingApply = false; break; }

                    if (usingValue.Value.operation == "=")
                    {
                        if (!rowValue.Equals(parsedValueResult.Value)) { doesUsingApply = false; break; }
                    }
                    else
                    {
                        doesUsingApply = false; break;
                    }
                }
                if (doesUsingApply) cellsThatApply.Add(context);
            }

            return Result<IAsyncEnumerable<Log>>.Success(TurnContextRowsToLogs(logManager, cellsThatApply, tableName, contextName, cellEnv));
        }

        private static async IAsyncEnumerable<Log> TurnContextRowsToLogs(ILogManager logManager, List<Row> cells, string tableName, string contextName, ContextEnvironment cellEnv)
        {
            foreach (var context in cells)
            {
                await foreach (var log in logManager.GetAllLogsForContextInstanceTable(contextName, tableName, cellEnv.GetIndexValues(context)))
                {
                    yield return log;
                }
            }
        }
    }
}
