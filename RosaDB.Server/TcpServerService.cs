using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RosaDB.Library.Server;

namespace RosaDB.Server;

public class TcpServerService(ILogger<TcpServerService> logger) : BackgroundService
{
    private readonly Library.Server.Server _server = new("127.0.0.1", 7575);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Starting TCP Server on 127.0.0.1:7575");
        
        try { await _server.Start(); }
        catch (Exception ex) { logger.LogError(ex, "TCP Server crashed"); }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping TCP Server");
        _server.Stop();
        return base.StopAsync(cancellationToken);
    }
}
