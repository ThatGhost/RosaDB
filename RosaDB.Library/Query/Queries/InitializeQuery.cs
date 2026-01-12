using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class InitializeQuery(RootManager rootManager, ICellManager cellManager, IDatabaseManager databaseManager, SessionState sessionState) : IQuery
{
    private const string SystemDatabaseName = "_system";
    private const string SessionIdColumnName = "sessionId";
    private const string LogTableName = "_logs";

    public async ValueTask<QueryResult> Execute()
    {
        var rootResult = await rootManager.InitializeRoot()
            .ThenAsync(() => InitilizeSystemDatabase())
            .ThenAsync(() => CreateLoggingTableAndCell());

        return rootResult.IsSuccess ? new QueryResult("RosaDB successfully initialized") : rootResult.Error;
    }

    private async Task<Result> InitilizeSystemDatabase()
    {
        return await rootManager.CreateDatabase(SystemDatabaseName)
            .Then<Database>(() => Database.Create(SystemDatabaseName))
            .Then(database =>
            {
                sessionState.CurrentDatabase = database;
                return Task.FromResult(Result.Success());
            });
    }

    private async Task<Result> CreateLoggingTableAndCell()
    {
        return await Task.FromResult(Column.Create(SessionIdColumnName, DataType.TEXT, isIndex: true))
            .ThenAsync<Column>(column => databaseManager.CreateCell(LogTableName, [column]))
            .Then<Column[]>(() =>
            {
                var logColumnResult = Column.Create("message", DataType.TEXT);
                if (logColumnResult.IsFailure) return logColumnResult.Error;

                var sessionIdColumnResult = Column.Create(SessionIdColumnName, DataType.TEXT);
                if (sessionIdColumnResult.IsFailure) return sessionIdColumnResult.Error;

                var timestampColumnResult = Column.Create("timestamp", DataType.TEXT);
                if (timestampColumnResult.IsFailure) return timestampColumnResult.Error;

                return Result<Column[]>.Success([sessionIdColumnResult.Value, logColumnResult.Value, timestampColumnResult.Value]);
            })
            .Then(columns => Table.Create(LogTableName, columns))
            .ThenAsync<Table>(table => cellManager.CreateTable(LogTableName, table));
    }
}