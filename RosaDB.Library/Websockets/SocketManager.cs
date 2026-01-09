using LightInject;
using RosaDB.Library.Server;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;

namespace RosaDB.Library.Websockets
{
    public class SocketManager
    {
        private readonly ConcurrentQueue<WebSocket> _sockets = new ConcurrentQueue<WebSocket>();
        private readonly ConcurrentDictionary<WebSocket, TaskCompletionSource<bool>> _socketTasks = new ConcurrentDictionary<WebSocket, TaskCompletionSource<bool>>();
        private readonly ServiceContainer _container;

        private readonly ISubscriptionManager _subscriptionManager;

        public SocketManager(ServiceContainer container)
        {
            _container = container;
            _subscriptionManager = _container.GetInstance<ISubscriptionManager>();
            var thread = new Thread(ProcessWebSockets);
            thread.IsBackground = true;
            thread.Start();
        }

        public Task AddSocket(WebSocket socket)
        {
            var tcs = new TaskCompletionSource<bool>();
            _socketTasks.TryAdd(socket, tcs);
            _sockets.Enqueue(socket);
            return tcs.Task;
        }

        private async void ProcessWebSockets()
        {
            while (true)
            {
                if (_sockets.TryDequeue(out var socket))
                {
                    if (_socketTasks.TryGetValue(socket, out var tcs))
                        await HandleWebSocketAsync(socket, tcs);
                }
                else Thread.Sleep(100);
            }
        }

        private async Task HandleWebSocketAsync(WebSocket webSocket, TaskCompletionSource<bool> tcs)
        {
            await using var scope = _container.BeginScope();
            WebsocketQueryPlanner queryPlanner = scope.GetInstance<WebsocketQueryPlanner>();
            var buffer = new byte[1024 * 4];
            try
            {
                while (webSocket.State == WebSocketState.Open)
                {
                    var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    if (result.MessageType == WebSocketMessageType.Text)
                    {
                        var message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                        var queryResult = await queryPlanner.ExecuteWebsocketQuery(message, webSocket);
                        var reply = $"{queryResult.Message}";
                        var replyBuffer = Encoding.UTF8.GetBytes(reply);
                        await webSocket.SendAsync(new ArraySegment<byte>(replyBuffer), WebSocketMessageType.Text, true, CancellationToken.None);
                    }
                    else if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None);
                    }
                }
            }
            catch
            {
                // Handle exceptions
            }
            finally
            {
                _socketTasks.TryRemove(webSocket, out _);
                _subscriptionManager.RemoveWebSocket(webSocket);
                webSocket.Dispose();
                tcs.SetResult(true);
            }
        }
    }
}
