namespace RosaDB.Library.Models;

public class Column
{
    public string Name { get; }
    public DataType DataType { get; }
    public bool IsPrimaryKey { get; }
    public bool IsIndex { get; }

    public Column(string name, DataType dataType, bool isPrimaryKey = false, bool isIndex = false)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be empty.", nameof(name));

        Name = name;
        DataType = dataType;
        IsPrimaryKey = isPrimaryKey;
        IsIndex = isIndex;
    }
}
