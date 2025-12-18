using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace RosaDB.Client.Client;

public class Client
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;

    public Client(string ipAddress, int port)
    {
        _client = new TcpClient(ipAddress, port);
        _stream = _client.GetStream();
    }

    public async Task<string> SendQueryAsync(string query)
    {
        var buffer = Encoding.UTF8.GetBytes(query);
        await _stream.WriteAsync(buffer, 0, buffer.Length);

        buffer = new byte[1024];
        var bytesRead = await _stream.ReadAsync(buffer, 0, buffer.Length);
        return Encoding.UTF8.GetString(buffer, 0, bytesRead);
    }
}
