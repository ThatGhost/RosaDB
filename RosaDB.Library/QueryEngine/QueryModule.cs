using RosaDB.Library.Core;
using RosaDB.Server;

namespace RosaDB.Library.QueryEngine;

public abstract class QueryModule
{
    public abstract Task<Result> Execute(ClientSession client, CancellationToken ct);
}