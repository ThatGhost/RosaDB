using RosaDB.Library.Models;
using System.Collections.Generic;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface ILogCondenser
    {
        List<Log> Condense(Queue<Log> logs);

        List<Log> Condense(IEnumerable<Log> logs);
    }
}
