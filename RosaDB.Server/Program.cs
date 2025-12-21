using RosaDB.Server;
using System.Net.WebSockets;
using System.Text;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHostedService<TcpServerService>();

var app = builder.Build();

app.UseWebSockets();

app.Map("/ws", async context =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        using var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        
        await using var wsStream = WebSocketStream.Create(webSocket, WebSocketMessageType.Text, true);
        using var reader = new StreamReader(wsStream, Encoding.UTF8);
        await using var writer = new StreamWriter(wsStream, Encoding.UTF8);
        writer.AutoFlush = true;

        while (await reader.ReadLineAsync() is { } message)
        {
            await writer.WriteLineAsync($"Echo: {message}");
        }
    }
    else
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
    }
});

app.MapGet("/", () => "RosaDB Server is running.");

await app.RunAsync();
