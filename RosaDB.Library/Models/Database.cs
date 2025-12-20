using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class Database
{
    public string Name { get; private init; } = "";

    public static Result<Database> Create(string name)
    {
        if(string.IsNullOrWhiteSpace(name)) return new Error(ErrorPrefixes.DataError, "Database name cannot be empty.");
        return new Database()
        {
            Name = name
        };
    }
}