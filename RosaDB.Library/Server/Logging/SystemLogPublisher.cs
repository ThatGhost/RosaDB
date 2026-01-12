using RosaDB.Library.Server.Interfaces; // For ISystemLogPublisher, LogLevel
using RosaDB.Library.Server.Logging;     // For ILogQueue, LogRecord
using System; // For Guid

namespace RosaDB.Library.Server.Logging
{
    public class SystemLogPublisher : ISystemLogPublisher
    {
        private readonly ILogQueue _logQueue;
        private readonly SessionState _sessionState; // Injected to get SessionId

        public SystemLogPublisher(ILogQueue logQueue, SessionState sessionState)
        {
            _logQueue = logQueue;
            _sessionState = sessionState;
        }

        public void Publish(LogLevel level, string message)
        {
            // Get the SessionId from the injected SessionState
            var sessionId = _sessionState.SessionId.ToString();

            var logRecord = new LogRecord(sessionId, level, message);
            _logQueue.Enqueue(logRecord);
        }
    }
}
