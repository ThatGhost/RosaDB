using System.Net.Sockets;
using System.Text;
using RosaDB.Library.QueryEngine;

namespace RosaDB.Server;

public class ClientSession
{
    private readonly TcpClient _client;
    private string? _databaseName;
    public string DatabaseName => _databaseName ?? "";

    public ClientSession(TcpClient client)
    {
        _client = client;
        _databaseName = null;
    }

    public async Task HandleClient()
    {
        var stream = _client.GetStream();
        var executor = new QueryExecutor();
        while (true)
        {
            var buffer = new byte[1024];
            var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
            if (bytesRead == 0)
            {
                break;
            }

            var query = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            var result = await executor.Execute(this, query, CancellationToken.None);

            var response = result.IsSuccess ? "Success" : result.Error.Message;
            var responseBuffer = Encoding.UTF8.GetBytes(response);
            await stream.WriteAsync(responseBuffer, 0, responseBuffer.Length);
        }
    }
    
    public void SetDatabase(string? usedDatabase) => _databaseName = usedDatabase;
}
