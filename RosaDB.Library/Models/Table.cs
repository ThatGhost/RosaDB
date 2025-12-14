namespace RosaDB.Library.Models;

public class Table
{
    public string Name { get; set; } = string.Empty;
    public Column[] Columns { get; set; } = [];
}