using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class Module
{
    public string Name { get; init; } = string.Empty;

    public static Result<Module> Create(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return new Error(ErrorPrefixes.DataError, "Module name cannot be empty.");

        return new Module()
        {
            Name = name
        };
    }
}