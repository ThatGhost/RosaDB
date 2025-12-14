namespace RosaDB.Library.Models;

public class Database
{
    public string Name { get; set; }
    public Cell[] Cells { get; set; }
    
    public Database(string name, Cell[] cells)
    {
        if(string.IsNullOrWhiteSpace(name))
            throw new ArgumentNullException(nameof(name), "Database name cannot be empty");
        
        Name = name;
        Cells = cells;
    }
}