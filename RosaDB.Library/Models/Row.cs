using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class Row
{
    private Dictionary<string, int> _columnIndexMap { get; } = [];
    
    public Dictionary<string, int> ColumnIndexMap => _columnIndexMap;
    
    public static Result<Row> Create(object?[] values, Column[] columns)
    {
        if (values.Length != columns.Length) return new Error(ErrorPrefixes.DataError, "Value count must match column count.");

        Row row = new Row()
        {
            Values = values,
            Columns = columns,
        };

        for (int i = 0; i < columns.Length; i++)
        {
            row._columnIndexMap[columns[i].Name] = i;
        }

        return row;
    }

    public object?[] Values { get; private init; } = [];
    public Column[] Columns { get; private init; } = [];

    public object? this[int index] => index < 0 || index >= Values.Length ? null : Values[index];

    public object? this[string columnName] => _columnIndexMap.TryGetValue(columnName, out int index) ? Values[index] : null;

    public Dictionary<string, object?> ToIndexDictionary()
    {
        Dictionary<string, object?> indexes = [];
        for (int i = 0; i < Columns.Length; i++)
        {
            if(Columns[i].IsIndex) indexes.Add(Columns[i].Name, Values[i]);
        }

        return indexes;
    }
}
