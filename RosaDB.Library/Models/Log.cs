namespace RosaDB.Library.Models;

public class Log
{
    public long Id { get; init; }
    public bool IsDeleted { get; init; }
    public DateTime Date { get; init; } = DateTime.Now;
    public byte[] TupleData { get; set; } = [];
    
    // Transient property to avoid deserialization during commit
    public List<(string Name, byte[] Value, bool IsPrimaryKey)>? IndexValues { get; set; }
}