using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query;
using System.Net.WebSockets;
using RosaDB.Library.Query.Queries;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.Websockets.Interfaces;

namespace RosaDB.Library.Websockets;

public class WebsocketQueryPlanner(QueryTokenizer queryTokenizer, ISubscriptionManager subscriptionManager, SessionState sessionState, RootManager rootManager)
{
    public async Task<QueryResult> ExecuteWebsocketQuery(string query, WebSocket webSocket)
    {
        var tokenizeResult = queryTokenizer.Tokenize(query);
        if (!tokenizeResult.TryGetValue(out var tokenLists)) return tokenizeResult.Error;

        if (tokenLists.Count > 1) return new Error(ErrorPrefixes.QueryParsingError, "Batch commands are not supported over websockets.");

        var tokens = tokenLists.FirstOrDefault();
        if (tokens == null || tokens.Length == 0) return new Error(ErrorPrefixes.QueryParsingError, "Empty query.");

        return tokens[0].ToUpperInvariant() switch
        {
            "USE" => await new UseQuery(tokens, sessionState, rootManager).Execute(),
            "SUBSCRIBE" => await subscriptionManager.HandleSubscribe(tokens, webSocket),
            "UNSUBSCRIBE" => await subscriptionManager.HandleUnsubscribe(tokens, webSocket),
            _ => new Error(ErrorPrefixes.QueryParsingError, "Unsupported websocket query type")
        };
    }
}
