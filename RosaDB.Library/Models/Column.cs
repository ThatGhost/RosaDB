using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class Column
{
    public string Name { get; private init; } = "";
    public DataType DataType { get; private init; } = DataType.INT;
    public bool IsPrimaryKey { get; private init; }
    public bool IsIndex { get; private init; }
    
    public object? MetaData { get; private init; }

    public static Result<Column> Create(string name, DataType dataType ,object? metadata = null, bool isPrimaryKey = false, bool isIndex = false)
    {
        if (string.IsNullOrWhiteSpace(name))
            return new Error(ErrorPrefixes.DataError, "Column name cannot be empty.");

        return new Column()
        {
            Name = name,
            DataType = dataType,
            IsIndex = isIndex,
            MetaData = metadata,
            IsPrimaryKey = isPrimaryKey
        };
    }

    private Column()
    {
        
    }
}
