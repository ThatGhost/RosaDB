using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using System.Net.WebSockets;

namespace RosaDB.Library.Websockets
{
    public interface ISubscriptionManager
    {
        Task<QueryResult> HandleSubscribe(string[] tokens, WebSocket webSocket);
        Task<QueryResult> HandleUnsubscribe(string[] tokens, WebSocket webSocket);
        Task NotifySubscriber(TableInstanceIdentifier tableIdentifier, Row newRow);
        void RemoveWebSocket(WebSocket webSocket);
    }
}
