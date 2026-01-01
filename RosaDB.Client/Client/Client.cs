using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

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
    private readonly JsonSerializerOptions Options = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };

    public Client(string ipAddress, int port)
    {
        _client = new TcpClient(ipAddress, port);
        _stream = _client.GetStream();
    }

    public async Task<ClientResponse?> SendQueryAsync(string query)
    {
        try
        {
            await SendQueryBytes(query);
            var responseLength = await GetResponseLenght();
            if(responseLength == -1) return null;

            var jsonResponse = await GetJsonResponse(responseLength);
            if(jsonResponse is null) return null;
            
            return JsonSerializer.Deserialize<ClientResponse>(jsonResponse, Options);
        }
        catch (Exception)
        {
            // Handle exceptions (e.g., connection lost)
            return null;
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
        while(totalBytesRead < responseLength)
        {
            var bytesRead = await _stream.ReadAsync(responseBuffer, totalBytesRead, responseLength - totalBytesRead);
            if (bytesRead == 0) return null;
            totalBytesRead += bytesRead;
        }

        return Encoding.UTF8.GetString(responseBuffer, 0, responseLength);
    }
}
