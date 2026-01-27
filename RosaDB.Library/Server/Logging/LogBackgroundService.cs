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
            //_backgroundTask = Task.Run(() => ProcessLogQueue(_cancellationTokenSource.Token));
        }

        public void Stop()
        {
            _cancellationTokenSource?.Cancel();
            _backgroundTask?.Wait();
        }

        private async Task ProcessLogQueue(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var record = logQueue.Dequeue(cancellationToken);
                    if (record != null)
                    {
                        await WriteLogRecord(record);
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex) { Console.Error.WriteLine($"Error in logger background service: {ex.Message}"); }
            }
        }

        private async Task WriteLogRecord(LogRecord record)
        {
            await using (var scope = serviceContainer.BeginScope())
            {
                var logManager = scope.GetInstance<ILogManager>();
                var sessionState = scope.GetInstance<SessionState>();
                var databaseManager = scope.GetInstance<IDatabaseManager>();
                var cellManager = scope.GetInstance<IContextManager>();

                var dbResult = databaseManager.GetDatabase(LogSystemInitializer.SystemDatabaseName);
                if (!dbResult.TryGetValue(out var db))
                {
                    Console.Error.WriteLine($"Failed to get system database: {dbResult.Error.Message}");
                    return;
                }
                sessionState.CurrentDatabase = db;

                var tableSchemaResult = await cellManager.GetColumnsFromTable(LogSystemInitializer.LogContextGroupName, LogSystemInitializer.LogTableName);
                if (!tableSchemaResult.TryGetValue(out var tableSchema)) return;

                var rowResult = Row.Create(
                    [record.SessionId, record.Message, record.Timestamp.ToString("o"), record.Level.ToString()],
                    tableSchema
                );
                if (!rowResult.TryGetValue(out var row))
                {
                    Console.Error.WriteLine($"Failed to create log row: {rowResult.Error.Message}");
                    return;
                }

                var serializeResult = RowSerializer.Serialize(row);
                if (!serializeResult.TryGetValue(out var serializedRow))
                {
                    Console.Error.WriteLine($"Failed to serialize log row: {serializeResult.Error.Message}");
                    return;
                }
                
                logManager.Put(LogSystemInitializer.LogContextGroupName, LogSystemInitializer.LogTableName, [sessionState.SessionId.ToString()], serializedRow, []);
                await logManager.Commit();
            }
        }

        public void Dispose()
        {
            Stop();
            _cancellationTokenSource?.Dispose();
        }
    }
}
