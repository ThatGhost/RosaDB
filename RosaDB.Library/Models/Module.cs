using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class Module
{
    public string Name { get; init; } = string.Empty;
    public List<Table> Tables { get; set; } = [];
    public List<Column> Columns { get; set; } = [];

    public static Result<Module> Create(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return new Error(ErrorPrefixes.DataError, "Module name cannot be empty.");

        return new Module()
        {
            Name = name
        };
    }
    
    public Table? GetTable(string name) => Tables.FirstOrDefault(t => t.Name == name);
    public Column? GetColumn(string name) => Columns.FirstOrDefault(c => c.Name == name);
}