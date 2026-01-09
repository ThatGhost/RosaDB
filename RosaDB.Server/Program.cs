using RosaDB.Server;
using RosaDB.Library.Websockets;
using LightInject;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHostedService<TcpServerService>();

var app = builder.Build();

app.UseWebSockets();

var socketManager = new SocketManager();

app.Map("/ws", async context =>
{
    // TODO: Add Authorization callback here
    if (context.WebSockets.IsWebSocketRequest)
    {
        var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        await socketManager.AddSocket(webSocket);
    }
    else
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
    }
});

app.MapGet("/", () => "RosaDB Server is running.");

await app.RunAsync();
