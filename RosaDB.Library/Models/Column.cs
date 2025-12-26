using RosaDB.Library.Core;
using System.Text.Json.Serialization;

namespace RosaDB.Library.Models;

public class Column
{
    public string Name { get; private init; } = "";
    public DataType DataType { get; private init; } = DataType.INT;
    public bool IsPrimaryKey { get; private init; }
    public bool IsIndex { get; private init; }
    public bool IsNullable { get; private init; } = true;
    public object? MetaData { get; private init; }

    [JsonConstructor]
    public Column(string name, DataType dataType, bool isPrimaryKey, bool isIndex, bool isNullable, object? metaData)
    {
        Name = name;
        DataType = dataType;
        IsPrimaryKey = isPrimaryKey;
        IsIndex = isIndex;
        IsNullable = isNullable;
        MetaData = metaData;
    }

    public static Result<Column> Create(string name, DataType dataType, object? metadata = null, bool isPrimaryKey = false, bool isIndex = false, bool isNullable = true)
    {
        if (string.IsNullOrWhiteSpace(name))
            return new Error(ErrorPrefixes.DataError, "Column name cannot be empty.");

        return new Column(name, dataType, isPrimaryKey, isIndex, isNullable, metadata);
    }

    private Column()
    {
        
    }
}
