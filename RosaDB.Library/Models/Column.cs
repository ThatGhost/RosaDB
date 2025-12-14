namespace RosaDB.Library.Models;

public class Column
{
    public string Name { get; }
    public DataType DataType { get; }
    public bool IsPrimaryKey { get; }

    public Column(string name, DataType dataType, bool isPrimaryKey = false)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be empty.", nameof(name));

        Name = name;
        DataType = dataType;
        IsPrimaryKey = isPrimaryKey;
    }
}
