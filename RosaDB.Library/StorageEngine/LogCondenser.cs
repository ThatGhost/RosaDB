using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine
{
    public class LogCondenser
    {
        public List<Log> Condense(Queue<Log> logs)
        {
            var condensedLogs = new Dictionary<long, Log>();
            foreach (var log in logs)
            {
                condensedLogs[log.Id] = log;
            }
            return condensedLogs.Values.ToList();
        }
    }
}
