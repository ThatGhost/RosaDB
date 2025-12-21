using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public interface IQuery
{
    public Task<QueryResult> Execute();
}