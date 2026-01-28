using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine;

public class WriteAheadLogCache
{
    public Dictionary<TableInstanceIdentifier, Queue<Log>> Logs { get; } = new();
}