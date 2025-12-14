namespace RosaDB.Library.Models;

public class Database
{
    public string Name { get; }
    
    public Database(string name)
    {
        if(string.IsNullOrWhiteSpace(name))
            throw new ArgumentNullException(nameof(name), "Database name cannot be empty");
        
        Name = name;
    }
}