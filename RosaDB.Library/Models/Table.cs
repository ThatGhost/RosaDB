using RosaDB.Library.Core;
using System.Text.Json.Serialization;

namespace RosaDB.Library.Models;

public class Table
{
    public string Name { get; private set; } = string.Empty;
    public Column[] Columns { get; private set; } = [];

    [JsonConstructor]
    public Table(string name, Column[] columns)
    {
        Name = name;
        Columns = columns;
    }

    public static Result<Table> Create(string name, Column[] columns)
    {
        if (string.IsNullOrWhiteSpace(name)) return new Error(ErrorPrefixes.DataError, "Table name cannot be empty.");

        return new Table(name, columns);
    }

    private Table()
    {
        
    }
}