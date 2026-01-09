using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.Websockets;

public class SubscriptionManager
{
    public async Task<QueryResult> HandleSubscribe(string[] tokens)
    {
        return new Error(ErrorPrefixes.QueryParsingError, "SUBSCRIBE not implemented yet");
    }

    public async Task<QueryResult> HandleUnsubscribe(string[] tokens)
    {
        return new Error(ErrorPrefixes.QueryParsingError, "UNSUBSCRIBE not implemented yet");
    }
}
