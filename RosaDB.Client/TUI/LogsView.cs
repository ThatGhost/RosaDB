using Terminal.Gui;
using System.Net.WebSockets;
using System.Text;

namespace RosaDB.Client.TUI;

public class LogsView : View
{
    private readonly TextView _logOutput;
    private readonly CancellationTokenSource _cts = new();
    private const int ServerPort = 9696;
    
    public LogsView()
    {
        Width = Dim.Fill();
        Height = Dim.Fill();

        _logOutput = new TextView()
        {
            X = 0,
            Y = 0,
            Width = Dim.Fill(),
            Height = Dim.Fill() - 1,
            ReadOnly = true,
        };
        Add(_logOutput);

        var copyButton = new Button("Copy Last 50 Logs")
        {
            X = 0,
            Y = Pos.AnchorEnd(1)
        };
        
        copyButton.Clicked += () =>
        {
            try
            {
                var text = _logOutput.Text.ToString();
                if (string.IsNullOrEmpty(text)) return;

                var lines = text.Split('\n');
                var last50 = lines.TakeLast(51); // Take 51 because usually the last line is empty due to trailing newline
                var contentToCopy = string.Join("\n", last50);
                
                Clipboard.Contents = contentToCopy;
                MessageBox.Query("Success", "Last 50 log lines copied to clipboard.", "Ok");
            }
            catch (Exception ex)
            {
                MessageBox.ErrorQuery("Error", $"Failed to copy: {ex.Message}", "Ok");
            }
        };
        Add(copyButton);

        Task.Run(ConnectAndChat);
    }

    private async Task ConnectAndChat()
    {
        try
        {
            Log($"Connecting to ws://127.0.0.1:{ServerPort}/ws ...");
            using var client = new ClientWebSocket();
            await client.ConnectAsync(new Uri($"ws://127.0.0.1:{ServerPort}/ws"), _cts.Token);
            Log("Connected!");

            await using var wsStream = WebSocketStream.Create(client, WebSocketMessageType.Text);
            using var reader = new StreamReader(wsStream, Encoding.UTF8);
            await using var writer = new StreamWriter(wsStream, Encoding.UTF8);
            writer.AutoFlush = true;

            _ = Task.Run(async () =>
            {
                while (!_cts.Token.IsCancellationRequested && client.State == WebSocketState.Open)
                {
                    try
                    {
                        await writer.WriteLineAsync("Hello");
                        await Task.Delay(5000, _cts.Token);
                    }
                    catch (Exception ex)
                    {
                        Log($"Send error: {ex.Message}");
                        break;
                    }
                }
            }, _cts.Token);

            // Read loop
            string? message;
            while ((message = await reader.ReadLineAsync()) != null)
            {
                Log($"Server: {message}");
            }
        }
        catch (Exception ex)
        {
            Log($"WebSocket Error: {ex.Message}");
            Log($"Make sure RosaDB.Server is running on port {ServerPort}.");
        }
    }

    private void Log(string message)
    {
        Application.MainLoop.Invoke(() =>
        {
            _logOutput.Text = $"{DateTime.Now:HH:mm:ss} - {message}\n" + _logOutput.Text;
        });
    }

    protected override void Dispose(bool disposing)
    {
        _cts.Cancel();
        base.Dispose(disposing);
    }
}