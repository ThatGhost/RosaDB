using System.Linq;
using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class QueryResult(string message, int rowsAffected = 0)
{
    public string Message { get; } = message;
    public List<Row> Rows { get; } = [];
    public IAsyncEnumerable<Row> RowStream { get; } = AsyncEnumerable.Empty<Row>();
    public int RowsAffected { get; } = rowsAffected;
    public bool IsStreaming { get; private set; } = false;

    public static implicit operator QueryResult(Error error) => new(error);

    public QueryResult(Error error) : this("Error:" + error.Prefix.Prefix + error.Message) { }

    public QueryResult(List<Row> rows) : this("Query executed successfully.")
    {
        Rows = rows;
    }

    public QueryResult(IAsyncEnumerable<Row> rowStream) : this("Query executed successfully.")
    {
        RowStream = rowStream;
        IsStreaming = true;
    }
}