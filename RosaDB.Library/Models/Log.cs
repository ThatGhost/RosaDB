namespace RosaDB.Library.Models;

public class Log
{
    public long Id { get; init; }
    public bool IsDeleted { get; init; }
    public DateTime Date { get; init; } = DateTime.Now;
    public byte[] TupleData { get; set; } = [];
}