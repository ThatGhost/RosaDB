using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine;

public class WriteAheadLogCache
{
    public Dictionary<string, Queue<Log>> Logs { get; } = new();
}