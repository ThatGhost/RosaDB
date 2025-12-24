using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class Cell
{
    public string Name { get; init; } = string.Empty;

    public static Result<Cell> Create(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return new Error(ErrorPrefixes.DataError, "Cell name cannot be empty.");

        return new Cell()
        {
            Name = name
        };
    }
}