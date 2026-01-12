using System;

namespace RosaDB.Library.Server.Interfaces
{
    public enum LogLevel
    {
        Trace = 0,
        Debug = 1,
        Information = 2,
        Warning = 3,
        Error = 4,
        Critical = 5,
        None = 6,
    }

    public interface ILogger
    {
        void Log(LogLevel logLevel, Exception exception);
        void Log(LogLevel logLevel, string? exception);
        void AddProperties(Dictionary<string, object> properties);
        void StartLogScope();
        void CommitLogScope(string? message = null);
        bool IsEnabled(LogLevel logLevel);
    }
}
