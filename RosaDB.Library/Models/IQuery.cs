using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public interface IQuery
{
    public ValueTask<QueryResult> Execute();
}