using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query;
using System.Net.WebSockets;

namespace RosaDB.Library.Websockets;

public class WebsocketQueryPlanner(QueryTokenizer queryTokenizer, ISubscriptionManager subscriptionManager)
{
    public async Task<QueryResult> ExecuteWebsocketQuery(string query, WebSocket webSocket)
    {
        var result = queryTokenizer.TokenizeQuery(query);
        if (result.IsFailure) return result.Error;

        return result.Value[0].ToUpperInvariant() switch
        {
            "SUBSCRIBE" => await subscriptionManager.HandleSubscribe(result.Value, webSocket),
            "UNSUBSCRIBE" => await subscriptionManager.HandleUnsubscribe(result.Value, webSocket),
            _ => new Error(ErrorPrefixes.QueryParsingError, "Unsupported websocket query type")
        };
    }
}
