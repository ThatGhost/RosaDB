using System.Collections.Concurrent;
using System.Threading; // Added for CancellationToken
using RosaDB.Library.Server.Logging;

namespace RosaDB.Library.Server.Logging
{
    public interface ILogQueue
    {
        void Enqueue(LogRecord record);
        LogRecord Dequeue(CancellationToken cancellationToken);
    }

    public class LogQueue : ILogQueue
    {
        private readonly BlockingCollection<LogRecord> _queue = new BlockingCollection<LogRecord>();

        public void Enqueue(LogRecord record)
        {
            _queue.Add(record);
        }

        public LogRecord Dequeue(CancellationToken cancellationToken)
        {
            return _queue.Take(cancellationToken);
        }
    }
}
