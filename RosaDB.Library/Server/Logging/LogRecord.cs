using System;
using RosaDB.Library.Server.Interfaces;

namespace RosaDB.Library.Server.Logging
{
    public class LogRecord
    {
        public string SessionId { get; }
        public LogLevel Level { get; }
        public string Message { get; }
        public DateTime Timestamp { get; }

        public LogRecord(string sessionId, LogLevel level, string message)
        {
            SessionId = sessionId;
            Level = level;
            Message = message;
            Timestamp = DateTime.UtcNow;
        }
    }
}
