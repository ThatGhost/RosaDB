using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class Context
{
    public string Name { get; init; } = string.Empty;

    public static Result<Context> Create(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return new Error(ErrorPrefixes.DataError, "Context name cannot be empty.");

        return new Context()
        {
            Name = name
        };
    }
}