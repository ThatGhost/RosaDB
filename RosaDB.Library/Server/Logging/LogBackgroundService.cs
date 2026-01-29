using LightInject;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Server.Logging
{
    public class LogBackgroundService(ILogQueue logQueue, IServiceContainer serviceContainer) : IDisposable
    {
        private CancellationTokenSource _cancellationTokenSource = new();
        private Task? _backgroundTask;

        public void Start()
        {
            _cancellationTokenSource = new CancellationTokenSource();
            _backgroundTask = Task.Run(() => ProcessLogQueue(_cancellationTokenSource.Token));
        }

        public void Stop()
        {
            _cancellationTokenSource.Cancel();
            _backgroundTask?.Wait();
        }

        private async Task ProcessLogQueue(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var record = logQueue.Dequeue(cancellationToken);
                    await WriteLogRecord(record);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex) { Console.Error.WriteLine($"Error in logger background service: {ex.Message}"); }
            }
        }

        private async Task WriteLogRecord(LogRecord record)
        {
            await using (var scope = serviceContainer.BeginScope())
            {
                var logWriter = scope.GetInstance<ILogWriter>();
                var sessionState = scope.GetInstance<SessionState>();
                var databaseManager = scope.GetInstance<IDatabaseManager>();
                var cellManager = scope.GetInstance<IModuleManager>();

                var dbResult = databaseManager.GetDatabase(LogSystemInitializer.SystemDatabaseName);
                if (!dbResult.TryGetValue(out var db)) return;
                sessionState.CurrentDatabase = db;

                var tableSchemaResult = await cellManager.GetColumnsFromTable(LogSystemInitializer.LogModuleGroupName, LogSystemInitializer.LogTableName);
                if (!tableSchemaResult.TryGetValue(out var tableSchema)) return;

                var rowResult = Row.Create(
                    [record.SessionId, record.Message, record.Timestamp.ToString("o"), record.Level.ToString()],
                    tableSchema
                );
                if (!rowResult.TryGetValue(out var row)) return;
                
                logWriter.Put(LogSystemInitializer.LogModuleGroupName, LogSystemInitializer.LogTableName, [sessionState.SessionId.ToString()], row.BSON, "");
                await logWriter.Commit();
            }
        }

        public void Dispose()
        {
            Stop();
            _cancellationTokenSource?.Dispose();
        }
    }
}
