using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query;
using System.Net.WebSockets;
using RosaDB.Library.Query.Queries;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.Websockets;

public class WebsocketQueryPlanner(QueryTokenizer queryTokenizer, ISubscriptionManager subscriptionManager, SessionState sessionState, RootManager rootManager)
{
    public async Task<QueryResult> ExecuteWebsocketQuery(string query, WebSocket webSocket)
    {
        var result = queryTokenizer.TokenizeQuery(query);
        if (result.IsFailure) return result.Error;

        return result.Value[0].ToUpperInvariant() switch
        {
            "USE" => await new UseQuery(result.Value, sessionState, rootManager).Execute(),
            "SUBSCRIBE" => await subscriptionManager.HandleSubscribe(result.Value, webSocket),
            "UNSUBSCRIBE" => await subscriptionManager.HandleUnsubscribe(result.Value, webSocket),
            _ => new Error(ErrorPrefixes.QueryParsingError, "Unsupported websocket query type")
        };
    }
}
