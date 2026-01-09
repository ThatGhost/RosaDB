using LightInject;
using System.Net;
using System.Net.Sockets;

namespace RosaDB.Library.Server;

public class Server
{
    private readonly TcpListener _listener;

    public Server(string ipAddress, int port)
    {
        _listener = new TcpListener(IPAddress.Parse(ipAddress), port);
    }

    public async Task Start(ServiceContainer serviceContainer)
    {
        _listener.Start();
        try
        {
            while (true)
            {
                var client = await _listener.AcceptTcpClientAsync();
                _ = Task.Run(async () =>
                {
                    await using var scope = serviceContainer.BeginScope();
                    var clientSession = scope.GetInstance<TcpClient, Scope, ClientSession>(client, scope);
                    await clientSession.HandleClient();
                });
            }
        }        
        catch (SocketException)
        {
            // Ignore the exception that is thrown when the listener is stopped.
        }
    }

    public void Stop()
    {
        _listener.Stop();
    }
}
