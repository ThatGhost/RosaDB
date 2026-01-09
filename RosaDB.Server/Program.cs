using RosaDB.Server;
using RosaDB.Library.Websockets;
using LightInject;
using RosaDB.Library.Server;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<TcpServerService>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<TcpServerService>());

var app = builder.Build();

app.UseWebSockets();

var container = new ServiceContainer();
Installer.Install(container);
var tcpService = app.Services.GetRequiredService<TcpServerService>();

tcpService!.serviceContainer = container;
var socketManager = new SocketManager(container);

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
