using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.StorageEngine
{
    public class LogCondenser : ILogCondenser
    {
        public List<Log> Condense(Queue<Log> logs)
        {
            var condensedLogs = new Dictionary<long, Log>();
            var deletedIds = new HashSet<long>();
            
            foreach (var log in logs)
            {
                if (deletedIds.Contains(log.Id))
                    continue;
                
                if(log.IsDeleted) deletedIds.Add(log.Id);
                condensedLogs[log.Id] = log;
            }

            return condensedLogs.Values.ToList();
        }

        public async Task<List<Log>> Condense(IAsyncEnumerable<Log> logs)
        {
            var condensedLogs = new Dictionary<long, Log>();
            var deletedIds = new HashSet<long>();

            await foreach (var log in logs)
            {
                if (deletedIds.Contains(log.Id))
                    continue;
                
                if (log.IsDeleted) deletedIds.Add(log.Id);
                condensedLogs[log.Id] = log;
            }

            return condensedLogs.Values.ToList();
        }
    }
}
