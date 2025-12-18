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
                if(condensedLogs.TryGetValue(log.Id, out var existingLog))
                {
                    if(log.Date > existingLog.Date) condensedLogs[log.Id] = log;
                }
                else condensedLogs[log.Id] = log;
            }

            return condensedLogs.Values.ToList();
        }
    }
}
