using LightInject;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Server.Logging
{
    public static class LogSystemInitializer
    {
        public const string SystemDatabaseName = "_system";
        public const string LogCellGroupName = "_sessions";
        public const string LogTableName = "_logs";
        public const string LogCellInstanceId = "sessionId";

        public static async Task<Result> InitializeAsync(IServiceContainer container)
        {
            using (var scope = container.BeginScope())
            {
                var cellManager = scope.GetInstance<ICellManager>();
                var sessionState = scope.GetInstance<SessionState>();
                var databaseManager = scope.GetInstance<IDatabaseManager>();

                var dbResult = databaseManager.GetDatabase(SystemDatabaseName);
                if (dbResult.IsFailure) return dbResult.Error;

                sessionState.CurrentDatabase = dbResult.Value;

                // 1. Ensure _sessions CellGroup exists
                var cellGroupEnvResult = await cellManager.GetEnvironment(LogCellGroupName);
                if (cellGroupEnvResult.IsFailure)
                {
                    var sessionIdColumn = Column.Create(LogCellInstanceId, DataType.TEXT, isIndex: true).Value;
                    var createCellGroupResult = await databaseManager.CreateCell(LogCellGroupName, [sessionIdColumn!]);
                    if (createCellGroupResult.IsFailure) return createCellGroupResult.Error;
                }

                // 2. Ensure _logs Table exists within _sessions CellGroup
                var tableSchemaResult = await cellManager.GetColumnsFromTable(LogCellGroupName, LogTableName);
                if (tableSchemaResult.IsFailure)
                {
                    var sessionIdCol = Column.Create(LogCellInstanceId, DataType.TEXT).Value;
                    var messageCol = Column.Create("message", DataType.TEXT).Value;
                    var timestampCol = Column.Create("timestamp", DataType.TEXT).Value;
                    var levelCol = Column.Create("level", DataType.TEXT).Value;

                    var table = Table.Create(LogTableName, [sessionIdCol!, messageCol!, timestampCol!, levelCol!]).Value;
                    var createTableResult = await cellManager.CreateTable(LogCellGroupName, table!);
                    if (createTableResult.IsFailure) return createTableResult.Error;
                }

                return Result.Success();
            }
        }
    }
}
