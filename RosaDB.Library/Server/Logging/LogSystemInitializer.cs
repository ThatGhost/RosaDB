using LightInject;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

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

                var envResult = await cellManager.GetEnvironment(LogCellGroupName);
                if (!envResult.TryGetValue(out var cellEnv)) return envResult.Error;

                var instanceHash = InstanceHasher.GenerateCellInstanceHash(new Dictionary<string, string> { { LogCellInstanceId, sessionState.SessionId.ToString() } });
                var existingCell = await cellManager.GetCellInstance(LogCellGroupName, instanceHash);

                if (existingCell.IsSuccess) return Result.Success();

                var rowCreateResult = Row.Create([LogCellInstanceId], cellEnv.Columns);
                if (!rowCreateResult.TryGetValue(out var row)) return rowCreateResult.Error;

                return await cellManager.CreateCellInstance(LogCellGroupName, instanceHash, row, cellEnv.Columns);
            }
        }
    }
}
