namespace RosaDB.Library.Models;

public interface IRow
{
    object? this[int index] { get; }
    object? this[string columnName] { get; }
    object?[] Values { get; }
    Column[] Columns { get; }
}

public class Row : IRow
{
    private readonly Dictionary<string, int> _columnIndexMap;

    public Row(object?[] values, Column[] columns)
    {
        if (values.Length != columns.Length)
            throw new ArgumentException("Value count must match column count.");

        Values = values;
        Columns = columns;
        _columnIndexMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < columns.Length; i++)
        {
            _columnIndexMap[columns[i].Name] = i;
        }
    }

    public object?[] Values { get; }
    public Column[] Columns { get; }

    public object? this[int index]
    {
        get
        {
            if (index < 0 || index >= Values.Length)
                throw new IndexOutOfRangeException($"Column index {index} is out of range.");
            return Values[index];
        }
    }

    public object? this[string columnName]
    {
        get
        {
            if (_columnIndexMap.TryGetValue(columnName, out int index))
            {
                return Values[index];
            }
            throw new KeyNotFoundException($"Column '{columnName}' not found in row.");
        }
    }
}
