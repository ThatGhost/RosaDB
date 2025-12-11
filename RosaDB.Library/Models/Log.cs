namespace RosaDB.Library.Models;

public class Log
{
    public long Id { get; set; }
    public bool IsDeleted { get; set; }
    public DateTime Date { get; set; } = DateTime.Now;
    public byte[] TupleData { get; set; } = [];
}