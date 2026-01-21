using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using RosaDB.Library.Validation;

namespace RosaDB.Library.Query.Queries;

public class DeleteQuery(
    string[] tokens,
    IContextManager cellManager,
    ILogManager logManager,
    IIndexManager indexManager,
    SessionState sessionState) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        // DELETE FROM <context>.<table> USING ... WHERE ...
        if (tokens[0].ToUpperInvariant() != "DELETE") return new Error(ErrorPrefixes.QueryParsingError, "Incorrect query type");

        var (_, fromIndex, _, usingIndex) = TokensToIndexesParser.ParseQueryTokens(tokens);
        var (contextName, tableName) = TokensToContextAndTableParser.TokensToContextAndName(tokens[fromIndex + 1]);
        
        var cellEnvResult = await cellManager.GetEnvironment(contextName);
        if (cellEnvResult.IsFailure)
        {
            throw new InvalidOperationException(cellEnvResult.Error.Message);
        }
        var cellEnv = cellEnvResult.Value;

        IAsyncEnumerable<Log> logs;
        if (usingIndex != -1)
        {
            var result = await UsingClauseProcessor.Process(tokens, cellManager, logManager, cellEnv);
            if(result.IsFailure) throw new InvalidOperationException(result.Error.Message);
            logs = result.Value;
        }
        else logs = logManager.GetAllLogsForContextTable(contextName, tableName);

        int count = 0;
        await foreach (var log in logs)
        {
            if (log.IndexValues is null)
            {
                continue;
            }

            var indexValues = new List<object?>();
            for (var i = 0; i < log.IndexValues.Count; i++)
            {
                var indexValue = log.IndexValues[i];
                var column = cellEnv.IndexColumns[i];

                var value = IndexKeyConverter.FromByteArray(indexValue.Value, column.DataType);
                indexValues.Add(value);
            }

            logManager.Delete(contextName, tableName, indexValues.ToArray()!, log.Id);
            count++;  
        }
        
        return new QueryResult("Delete was successful", count);
    }
    

}