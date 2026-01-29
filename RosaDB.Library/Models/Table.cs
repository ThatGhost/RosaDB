using RosaDB.Library.Core;
using System.Text.Json.Serialization;

namespace RosaDB.Library.Models;

[method: JsonConstructor]
public class Table(string name, List<Column> columns)
{
    public string Name { get; } = name;
    public List<Column> Columns { get; } = columns;
    
    public static Result<Table> Create(string name, List<Column> columns)
    {
        if (string.IsNullOrWhiteSpace(name)) return new Error(ErrorPrefixes.DataError, "Table name cannot be empty.");

        return new Table(name, columns.ToList());
    }
    
    public Column? GetColumn(string name) => Columns.FirstOrDefault(c => c.Name == name);
}