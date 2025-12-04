using System.Net;
using System.Net.Sockets;

namespace RosaDB.Server;

public class Server
{
    private readonly TcpListener _listener;

    public Server(string ipAddress, int port)
    {
        _listener = new TcpListener(IPAddress.Parse(ipAddress), port);
    }

    public async Task Start()
    {
        _listener.Start();
        try
        {
            while (true)
            {
                var client = await _listener.AcceptTcpClientAsync();
                var clientSession = new ClientSession(client);
                Task.Run(() => clientSession.HandleClient());
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
