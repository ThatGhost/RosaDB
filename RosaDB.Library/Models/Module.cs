using System.Text.Json.Serialization;
using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

[method: JsonConstructor]
public class Module(string name, List<Table> tables, List<Column> columns)
{
    public string Name { get; } = name;
    public List<Table> Tables { get; } = tables;
    public List<Column> Columns { get; } = columns;

    public static Result<Module> Create(string name, List<Column> columns)
    {
        if (string.IsNullOrWhiteSpace(name))
            return new Error(ErrorPrefixes.DataError, "Module name cannot be empty.");

        return new Module(name, [] ,columns);
    }
    
    public Table? GetTable(string name) => Tables.FirstOrDefault(t => t.Name == name);
    public Column? GetColumn(string name) => Columns.FirstOrDefault(c => c.Name == name);
}