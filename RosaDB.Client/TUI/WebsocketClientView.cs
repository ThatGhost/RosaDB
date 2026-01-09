using Terminal.Gui;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Net.Sockets;
using System.IO;

namespace RosaDB.Client.TUI
{
    public class WebsocketClientView : View
    {
        private readonly TextView _logOutput;
        private readonly TextField _queryInput;
        private readonly CancellationTokenSource _cts = new();
        private const int ServerPort = 9696;
        private ClientWebSocket? _client;

        public WebsocketClientView()
        {
            Width = Dim.Fill();
            Height = Dim.Fill();

            var queryLabel = new Label("Query:")
            {
                X = 0,
                Y = 0
            };
            _queryInput = new TextField("")
            {
                X = 0,
                Y = 1,
                Width = Dim.Fill() - 15
            };
            var sendButton = new Button("Send")
            {
                X = Pos.Right(_queryInput) + 1,
                Y = 1
            };
            sendButton.Clicked += SendQuery;

            _logOutput = new TextView()
            {
                X = 0,
                Y = 3,
                Width = Dim.Fill(),
                Height = Dim.Fill(),
                ReadOnly = true
            };

            Add(queryLabel, _queryInput, sendButton, _logOutput);

            Task.Run(ConnectAndListen);
        }

        private async Task ConnectAndListen()
        {
            try
            {
                Log($"Connecting to ws://127.0.0.1:{ServerPort}/ws ...");
                _client = new ClientWebSocket();
                await _client.ConnectAsync(new Uri($"ws://127.0.0.1:{ServerPort}/ws"), _cts.Token);
                Log("Connected!");

                var buffer = new byte[1024 * 4];
                while (_client.State == WebSocketState.Open)
                {
                    var receiveBuffer = new ArraySegment<byte>(new byte[1024 * 4]);
                    var result = await _client.ReceiveAsync(receiveBuffer, CancellationToken.None);
                    if (result.MessageType == WebSocketMessageType.Text)
                    {
                        if (receiveBuffer.Array is not null)
                        {
                            var message = Encoding.UTF8.GetString(receiveBuffer.Array, 0, result.Count);
                            Log($"Server: {message}");
                        }
                    }
                    else if (result.MessageType == WebSocketMessageType.Binary)
                    {
                        if (receiveBuffer.Array is not null)
                        {
                            // First message is the length
                            var length = BitConverter.ToInt32(receiveBuffer.Array, 0);

                            // Second message is the payload
                            var payloadBuffer = new ArraySegment<byte>(new byte[length]);
                            result = await _client.ReceiveAsync(payloadBuffer, CancellationToken.None);

                            if (payloadBuffer.Array is not null)
                            {
                                var json = Encoding.UTF8.GetString(payloadBuffer.Array, 0, result.Count);
                        
                                // pretty print json
                                using var jDoc = JsonDocument.Parse(json);
                                var prettyJson = JsonSerializer.Serialize(jDoc.RootElement, new JsonSerializerOptions { WriteIndented = true });

                                Log($"Received data:\n{prettyJson}");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"WebSocket Error: {ex.Message}");
            }
        }

        private async void SendQuery()
        {
            if (_client?.State != WebSocketState.Open)
            {
                Log("Not connected.");
                return;
            }

            var query = _queryInput.Text.ToString();
            if (string.IsNullOrWhiteSpace(query))
            {
                return;
            }

            var queryBytes = Encoding.UTF8.GetBytes(query);
            await _client.SendAsync(new ArraySegment<byte>(queryBytes), WebSocketMessageType.Text, true, CancellationToken.None);
            Log($"Sent: {query}");
        }

        private void Log(string message)
        {
            Application.MainLoop.Invoke(() =>
            {
                _logOutput.Text += $"{DateTime.Now:HH:mm:ss} - {message}\n";
            });
        }

        protected override void Dispose(bool disposing)
        {
            _cts.Cancel();
            _client?.Dispose();
            base.Dispose(disposing);
        }
    }
}
