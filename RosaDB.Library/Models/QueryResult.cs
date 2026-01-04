using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class QueryResult(string message, int rowsAffected = 0)
{
    public string Message { get; } = message;
    public List<Row> Rows { get; } = [];
    public int RowsAffected { get; } = rowsAffected;

    public static implicit operator QueryResult(Error error) => new(error);

    public QueryResult(Error error) : this(error.Prefix.Prefix + error.Message) { }

    public QueryResult(List<Row> rows) : this("Query executed successfully.")
    {
        Rows = rows;
    }
}