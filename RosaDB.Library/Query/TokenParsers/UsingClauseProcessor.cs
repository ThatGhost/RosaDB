using System.Collections.ObjectModel;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Query.Queries
{
    public static class UsingClauseProcessor
    {
        public static async Task<Result<IAsyncEnumerable<Log>>> Process(
            string[] tokens,
            IModuleManager cellManager,
            ILogReader logReader,
            ModuleEnvironment cellEnv)
        {
            var cellIndexesResult = await GetIndexHashesFromUsing(tokens, cellManager, cellEnv);
            if(cellIndexesResult.IsFailure) return cellIndexesResult.Error;
            var (_, fromIndex, _, _) = TokensToIndexesParser.ParseQueryTokens(tokens);
            var (moduleName, tableName) = TokensToModuleAndTableParser.TokensToModuleAndName(tokens[fromIndex + 1]);

            List<Row> cellsThatApply = [];
            foreach (var cellIndex in cellIndexesResult.Value)
            {
                var cellInstanceResult = await cellManager.GetModuleInstance(moduleName, cellIndex);
                if (cellInstanceResult.IsFailure) return cellInstanceResult.Error;
                cellsThatApply.Add(cellInstanceResult.Value);
            }

            return Result<IAsyncEnumerable<Log>>.Success(TurnModuleRowsToLogs(logReader, cellsThatApply, tableName, moduleName));
        }

        private static async IAsyncEnumerable<Log> TurnModuleRowsToLogs(ILogReader logReader, List<Row> cells, string tableName, string moduleName)
        {
            foreach (var module in cells)
            {
                await foreach (var log in logReader.GetAllLogsForModuleInstanceTable(moduleName, tableName, module.InstanceHash))
                {
                    yield return log;
                }
            }
        }

        public static async Task<Result<List<List<object?>>>> GetIndexValuesFromUsing(string[] tokens,
            IModuleManager cellManager,
            ModuleEnvironment cellEnv)
        {
            var (_, fromIndex, whereIndex, usingIndex) = TokensToIndexesParser.ParseQueryTokens(tokens);
            var (moduleName, _) = TokensToModuleAndTableParser.TokensToModuleAndName(tokens[fromIndex + 1]);
            
            var endIndex = whereIndex != -1 ? whereIndex : tokens.Length - 1;
            var usingTokens = tokens[(usingIndex + 1)..endIndex];

            var usingValues = new Dictionary<string, (string value, string operation)>();
            for (int i = 0; i < usingTokens.Length; i += 4) usingValues[usingTokens[i]] = new(usingTokens[i + 2], usingTokens[i + 1]);

            // check if all and only index columns are present for module then use the module instance
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

                return new List<List<object?>> { indexValues.ToList()! };
            }

            // if it's not the indexes then get all the module instances and concat all the conforming cells
            var cellsResult = await cellManager.GetAllModuleInstances(moduleName);
            if (cellsResult.IsFailure) return cellsResult.Error;

            List<List<object?>> cellsThatApply = [];
            foreach (Row module in cellsResult.Value)
            {
                bool doesUsingApply = true;
                foreach (var usingValue in usingValues)
                {
                    var columnIndex = Array.FindIndex(module.Columns, c => c.Name.Equals(usingValue.Key, StringComparison.OrdinalIgnoreCase));
                    if (columnIndex == -1) { doesUsingApply = false; break; }

                    var rowValue = module.Values[columnIndex];
                    if (rowValue == null) { doesUsingApply = false; break; }

                    var parsedValueResult = TokensToDataParser.Parse(usingValue.Value.value, module.Columns[columnIndex].DataType);
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
                if (doesUsingApply) cellsThatApply.Add(cellEnv.GetIndexValues(module).ToList());
            }

            return cellsThatApply;
        }
        
        public static async Task<Result<List<string>>> GetIndexHashesFromUsing(string[] tokens,
            IModuleManager cellManager,
            ModuleEnvironment cellEnv)
        {
            var (_, fromIndex, whereIndex, usingIndex) = TokensToIndexesParser.ParseQueryTokens(tokens);
            var (moduleName, _) = TokensToModuleAndTableParser.TokensToModuleAndName(tokens[fromIndex + 1]);
            
            var endIndex = whereIndex != -1 ? whereIndex : tokens.Length - 1;
            var usingTokens = tokens[(usingIndex + 1)..endIndex];

            var usingValues = new Dictionary<string, (string value, string operation)>();
            for (int i = 0; i < usingTokens.Length; i += 4) usingValues[usingTokens[i]] = new(usingTokens[i + 2], usingTokens[i + 1]);

            // check if all and only index columns are present for module then use the module instance
            var indexStringValues = usingValues.Keys.Where(u => cellEnv.IndexColumns.Select(i => i.Name).Contains(u)).ToArray();
            if (usingValues.Count == cellEnv.IndexColumns.Length && indexStringValues.Length == cellEnv.IndexColumns.Length)
            {
                Dictionary<string, string> indexValues = [];
                foreach (var cellEnvIndexColumn in cellEnv.IndexColumns)
                {
                    if(!indexValues.ContainsKey(cellEnvIndexColumn.Name)) break;
                    indexValues[cellEnvIndexColumn.Name] = usingValues[cellEnvIndexColumn.Name].value;
                }

                return new List<string>{InstanceHasher.GenerateModuleInstanceHash(indexValues)};
            }

            // if it's not the indexes then get all the module instances and concat all the conforming cells
            var cellsResult = await cellManager.GetAllModuleInstances(moduleName); // TODO needs to become a stream
            if (cellsResult.IsFailure) return cellsResult.Error;

            List<string> cellsThatApply = [];
            foreach (Row module in cellsResult.Value)
            {
                bool doesUsingApply = true;
                foreach (var usingValue in usingValues)
                {
                    var columnIndex = Array.FindIndex(module.Columns, c => c.Name.Equals(usingValue.Key, StringComparison.OrdinalIgnoreCase));
                    if (columnIndex == -1) { doesUsingApply = false; break; }

                    var rowValue = module.Values[columnIndex];
                    if (rowValue == null) { doesUsingApply = false; break; }

                    var parsedValueResult = TokensToDataParser.Parse(usingValue.Value.value, module.Columns[columnIndex].DataType);
                    if (parsedValueResult.IsFailure) { doesUsingApply = false; break; }

                    if (usingValue.Value.operation == "=" && rowValue.Equals(parsedValueResult.Value)) continue;

                    doesUsingApply = false; break;
                }

                if (!doesUsingApply) continue;
                cellsThatApply.Add(module.InstanceHash);
            }

            return cellsThatApply;
        }
    }
}
