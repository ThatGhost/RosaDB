using LightInject;
using System.Net;
using System.Net.Sockets;

namespace RosaDB.Library.Server;

public class Server
{
    private readonly TcpListener _listener;
    private readonly ServiceContainer _container;

    public Server(string ipAddress, int port)
    {
        _listener = new TcpListener(IPAddress.Parse(ipAddress), port);
        _container = new ServiceContainer();
        Installer.Install(_container);
    }

    public async Task Start()
    {
        _listener.Start();
        try
        {
            while (true)
            {
                var client = await _listener.AcceptTcpClientAsync();
                _ = Task.Run(async () =>
                {
                    await using var scope = _container.BeginScope();
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
        _container.Dispose();
    }
}
