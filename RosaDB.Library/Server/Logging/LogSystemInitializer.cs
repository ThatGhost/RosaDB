using LightInject;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Server.Logging
{
    public static class LogSystemInitializer
    {
        public const string SystemDatabaseName = "_system";
        public const string LogContextGroupName = "_sessions";
        public const string LogTableName = "_logs";
        public const string LogContextInstanceId = "sessionId";

        public static async Task<Result> InitializeAsync(IServiceContainer container)
        {
            using (var scope = container.BeginScope())
            {
                var cellManager = scope.GetInstance<IContextManager>();
                var sessionState = scope.GetInstance<SessionState>();
                var databaseManager = scope.GetInstance<IDatabaseManager>();

                var dbResult = databaseManager.GetDatabase(SystemDatabaseName);
                if (dbResult.IsFailure) return dbResult.Error;

                sessionState.CurrentDatabase = dbResult.Value;

                // 1. Ensure _sessions ContextGroup exists
                var cellGroupEnvResult = await cellManager.GetEnvironment(LogContextGroupName);
                if (cellGroupEnvResult.IsFailure)
                {
                    var sessionIdColumn = Column.Create(LogContextInstanceId, DataType.TEXT, isIndex: true).Value;
                    var createContextGroupResult = await databaseManager.CreateContext(LogContextGroupName, [sessionIdColumn!]);
                    if (createContextGroupResult.IsFailure) return createContextGroupResult.Error;
                }

                // 2. Ensure _logs Table exists within _sessions ContextGroup
                var tableSchemaResult = await cellManager.GetColumnsFromTable(LogContextGroupName, LogTableName);
                if (tableSchemaResult.IsFailure)
                {
                    var sessionIdCol = Column.Create(LogContextInstanceId, DataType.TEXT).Value;
                    var messageCol = Column.Create("message", DataType.TEXT).Value;
                    var timestampCol = Column.Create("timestamp", DataType.TEXT).Value;
                    var levelCol = Column.Create("level", DataType.TEXT).Value;

                    var table = Table.Create(LogTableName, [sessionIdCol!, messageCol!, timestampCol!, levelCol!]).Value;
                    var createTableResult = await cellManager.CreateTable(LogContextGroupName, table!);
                    if (createTableResult.IsFailure) return createTableResult.Error;
                }

                return Result.Success();
            }
        }
    }
}
