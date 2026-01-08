using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace RosaDB.Client.Client;

public class ClientResponse
{
    public string Message { get; set; } = "";
    public int RowsAffected { get; set; }
    public List<Dictionary<string, object?>>? Rows { get; set; }
    public double DurationMs { get; set; }
}

public class Client
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public Client(string ipAddress, int port)
    {
        _client = new TcpClient(ipAddress, port);
        _stream = _client.GetStream();
    }

    public async IAsyncEnumerable<ClientResponse> SendQueryAndStreamAsync(string query)
    {
        await SendQueryBytes(query);

        while (true)
        {
            var responseLength = await GetResponseLenght();
            if (responseLength == -1) yield break;

            var jsonResponse = await GetJsonResponse(responseLength);
            if (jsonResponse is null) yield break;

            var response = JsonSerializer.Deserialize<ClientResponse>(jsonResponse, Options);
            if (response is null) yield break;

            yield return response;

            if (response.Message != "Row stream") break;
        }
    }

    private async Task SendQueryBytes(string query)
    {
        var queryBytes = Encoding.UTF8.GetBytes(query);
        var lengthBytes = BitConverter.GetBytes(queryBytes.Length);
        await _stream.WriteAsync(lengthBytes);
        await _stream.WriteAsync(queryBytes);
    }

    private async Task<int> GetResponseLenght()
    {
        var responseLengthBuffer = new byte[4];
        var bytesRead = await _stream.ReadAsync(responseLengthBuffer, 0, 4);
        if (bytesRead < 4) return -1;
        return BitConverter.ToInt32(responseLengthBuffer, 0);
    }

    private async Task<string?> GetJsonResponse(int responseLength)
    {
        var responseBuffer = new byte[responseLength];
        var totalBytesRead = 0;
        while (totalBytesRead < responseLength)
        {
            var bytesRead = await _stream.ReadAsync(responseBuffer, totalBytesRead, responseLength - totalBytesRead);
            if (bytesRead == 0) return null;
            totalBytesRead += bytesRead;
        }

        return Encoding.UTF8.GetString(responseBuffer, 0, responseLength);
    }
}
