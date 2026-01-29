using LightInject;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Server.Logging
{
    public static class LogSystemInitializer
    {
        public const string SystemDatabaseName = "_system";
        public const string LogModuleGroupName = "_sessions";
        public const string LogTableName = "_logs";
        public const string LogModuleInstanceId = "sessionId";

        public static async Task<Result> InitializeAsync(IServiceContainer container)
        {
            using (var scope = container.BeginScope())
            {
                var cellManager = scope.GetInstance<IModuleManager>();
                var sessionState = scope.GetInstance<SessionState>();
                var databaseManager = scope.GetInstance<IDatabaseManager>();

                var dbResult = databaseManager.GetDatabase(SystemDatabaseName);
                if (dbResult.IsFailure) return dbResult.Error;

                sessionState.CurrentDatabase = dbResult.Value;

                // 1. Ensure _sessions ModuleGroup exists
                var cellGroupEnvResult = await cellManager.GetEnvironment(LogModuleGroupName);
                if (cellGroupEnvResult.IsFailure)
                {
                    var sessionIdColumn = Column.Create(LogModuleInstanceId, DataType.TEXT, isIndex: true).Value;
                    var createModuleGroupResult = await databaseManager.CreateModule(LogModuleGroupName, [sessionIdColumn!]);
                    if (createModuleGroupResult.IsFailure) return createModuleGroupResult.Error;
                }

                // 2. Ensure _logs Table exists within _sessions ModuleGroup
                var tableSchemaResult = await cellManager.GetColumnsFromTable(LogModuleGroupName, LogTableName);
                if (tableSchemaResult.IsFailure)
                {
                    var sessionIdCol = Column.Create(LogModuleInstanceId, DataType.TEXT).Value;
                    var messageCol = Column.Create("message", DataType.TEXT).Value;
                    var timestampCol = Column.Create("timestamp", DataType.TEXT).Value;
                    var levelCol = Column.Create("level", DataType.TEXT).Value;

                    var table = Table.Create(LogTableName, [sessionIdCol!, messageCol!, timestampCol!, levelCol!]).Value;
                    var createTableResult = await cellManager.CreateTable(LogModuleGroupName, table!);
                    if (createTableResult.IsFailure) return createTableResult.Error;
                }

                return Result.Success();
            }
        }
    }
}
