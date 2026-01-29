using RosaDB.Server;
using RosaDB.Library.Websockets;
using LightInject;
using RosaDB.Library.Server;
using RosaDB.Library.Server.Logging;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<TcpServerService>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<TcpServerService>());

var app = builder.Build();

app.UseWebSockets();

var container = new ServiceContainer();
Installer.Install(container);

// ---- START LOGGING SERVICE ----
var logService = container.GetInstance<LogBackgroundService>();

//logService.Start();

// Ensure the logger is stopped gracefully on application shutdown
app.Lifetime.ApplicationStopping.Register(() => logService.Stop());
// ---- END LOGGING SERVICE ----

var tcpService = app.Services.GetRequiredService<TcpServerService>();

tcpService!.serviceContainer = container;
var socketManager = new SocketManager(container);

app.Map("/ws", async module =>
{
    // TODO: Add Authorization callback here
    if (module.WebSockets.IsWebSocketRequest)
    {
        var webSocket = await module.WebSockets.AcceptWebSocketAsync();
        await socketManager.AddSocket(webSocket);
    }
    else
    {
        module.Response.StatusCode = StatusCodes.Status400BadRequest;
    }
});

app.MapGet("/", () => "RosaDB Server is running.");

await app.RunAsync();
