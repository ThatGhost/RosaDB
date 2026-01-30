using System.Text.Json.Serialization;
using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

[method: JsonConstructor]
public class Column(string name, DataType dataType, bool isPrimaryKey, bool isIndex, bool isNullable)
{
    public string Name { get; private init; } = name;
    public DataType DataType { get; private init; } = dataType;
    public bool IsPrimaryKey { get; private init; } = isPrimaryKey;
    public bool IsIndex { get; private init; } = isIndex;
    public bool IsNullable { get; private init; } = isNullable;

    public static Result<Column> Create(string name, DataType dataType, bool isPrimaryKey = false, bool isIndex = false, bool isNullable = true)
    {
        if (string.IsNullOrWhiteSpace(name))
            return new Error(ErrorPrefixes.DataError, "Column name cannot be empty.");
        
        return new Column(name, dataType, isPrimaryKey, isIndex, isNullable);
    }
}
