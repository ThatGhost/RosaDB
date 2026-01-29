using RosaDB.Library.Core;
using System.Text.Json.Serialization;

namespace RosaDB.Library.Models;

public class Table
{
    public string Name { get; private set; } = string.Empty;
    public List<Column> Columns { get; private set; } = [];
    private Table() { }
    
    public static Result<Table> Create(string name, Column[] columns)
    {
        if (string.IsNullOrWhiteSpace(name)) return new Error(ErrorPrefixes.DataError, "Table name cannot be empty.");

        return new Table
        {
            Name = name,
            Columns = columns.ToList()
        };
    }
    
    public Column? GetColumn(string name) => Columns.FirstOrDefault(c => c.Name == name);
}