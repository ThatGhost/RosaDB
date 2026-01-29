using RosaDB.Library.Core;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Models;

public class Row
{
    public object?[] Values { get; private init; } = [];
    public Column[] Columns { get; private init; } = [];
    public string InstanceHash { get; private init; } = "";
    public byte[] BSON { get; private init; } = [];
    
    public static Result<Row> Create(object?[] values, Column[] columns)
    {
        if (values.Length != columns.Length) return new Error(ErrorPrefixes.DataError, "Value count must match column count.");

        var bsonResult = RowSerializer.Serialize(values, columns);
        if (bsonResult.IsFailure) return bsonResult.Error;
        
        Row row = new Row()
        {
            Values = values,
            Columns = columns,
            InstanceHash = GenerateInstanceHash(values, columns),
            BSON = bsonResult.Value
        };
        
        return row;
    }

    public object? GetValue(string columnName)
    {
        for (int i = 0; i < Columns.Length; i++)
        {
            if(columnName == Columns[i].Name) return Values[i];
        }

        return null;
    }

    private static string GenerateInstanceHash(object?[] values, Column[] columns)
    {
        var indexColumns = new Dictionary<string, string>();
        for (var index = 0; index < columns.Length; index++)
        {
            var column = columns[index];
            if(column.IsIndex || column.IsPrimaryKey) indexColumns.Add(column.Name, values[index]?.ToString() ?? "");
        }
        return InstanceHasher.GenerateModuleInstanceHash(indexColumns);
    }
}
