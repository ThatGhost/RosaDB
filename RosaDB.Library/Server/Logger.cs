using RosaDB.Library.Server.Interfaces;
using RosaDB.Library.StorageEngine.Interfaces;
using System;

namespace RosaDB.Library.Server
{
    public class Logger(ILogManager logManager, SessionState sessionState) : ILogger
    {
        private readonly Dictionary<string, object> logScopeProperties = new();

        private const string SessionIdPropertyName = "sessionId";
        private const string TimestampPropertyName = "timestamp";
        private const string LogLevelPropertyName = "logLevel";
        private const string MessagePropertyName = "message";
        private const string cellName = "_logs";
        private const string DatabaseName = "_system";

        public bool IsEnabled(LogLevel logLevel)
        {
            // For now, enable all log levels. This could be configured later.
            return true;
        }

        public void AddProperties(Dictionary<string, object> properties)
        {
            throw new NotImplementedException();
        }

        public void CommitLogScope(string? message)
        {
            throw new NotImplementedException();
        }

        public void Log(LogLevel logLevel, Exception exception)
        {
            Log(logLevel, "Exception triggerd: " + exception.Message + $"\n{exception.StackTrace}");
        }

        public void Log(LogLevel logLevel, string? exception)
        {
            
        }

        public void StartLogScope()
        {
            throw new NotImplementedException();
        }
    }
}
