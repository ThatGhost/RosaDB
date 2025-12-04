using RosaDB.Library.Core;

namespace RosaDB.Library.QueryEngine;

public abstract class QueryModule
{
    public abstract Task<Result> Excecute(CancellationToken ct);
}