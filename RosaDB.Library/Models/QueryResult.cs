using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class QueryResult
{
    public string Message { get; init; }
    public List<Row> Rows { get; init; } = [];
    public int RowsAffected { get; init; } = 0;
    
    public static implicit operator QueryResult(Error error) => new(error);

    private QueryResult(Error error)
    {
        Message = error.Message;
    }

    public QueryResult(string message)
    {
        Message = message;
    }

    public QueryResult(List<Row> rows)
    {
        Message = "Query executed successfully.";
    }
}