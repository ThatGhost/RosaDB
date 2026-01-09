using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query;

namespace RosaDB.Library.Websockets;

public class WebsocketQueryPlanner(QueryTokenizer queryTokenizer, SubscriptionManager subscriptionManager)
{
    public async Task<QueryResult> ExecuteWebsocketQuery(string query)
    {
        var result = queryTokenizer.TokenizeQuery(query);
        if (result.IsFailure) return result.Error;

        return result.Value[0] switch
        {
            "SUBSCRIBE" => await subscriptionManager.HandleSubscribe(result.Value),
            "UNSUBSCRIBE" => await subscriptionManager.HandleUnsubscribe(result.Value),
            _ => new Error(ErrorPrefixes.QueryParsingError, "Unsupported websocket query type")
        };
    }
}
