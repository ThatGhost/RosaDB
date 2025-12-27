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

    public Client(string ipAddress, int port)
    {
        _client = new TcpClient(ipAddress, port);
        _stream = _client.GetStream();
    }

    public async Task<ClientResponse?> SendQueryAsync(string query)
    {
        try
        {
            // Send query with length prefix
            var queryBytes = Encoding.UTF8.GetBytes(query);
            var lengthBytes = BitConverter.GetBytes(queryBytes.Length);
            await _stream.WriteAsync(lengthBytes);
            await _stream.WriteAsync(queryBytes);

            // Read response with length prefix
            var responseLengthBuffer = new byte[4];
            var bytesRead = await _stream.ReadAsync(responseLengthBuffer, 0, 4);
            if (bytesRead < 4) return null; // Or throw an exception

            var responseLength = BitConverter.ToInt32(responseLengthBuffer, 0);
            var responseBuffer = new byte[responseLength];
            var totalBytesRead = 0;
            while(totalBytesRead < responseLength)
            {
                bytesRead = await _stream.ReadAsync(responseBuffer, totalBytesRead, responseLength - totalBytesRead);
                if (bytesRead == 0) return null; // Connection closed prematurely
                totalBytesRead += bytesRead;
            }

            var jsonResponse = Encoding.UTF8.GetString(responseBuffer, 0, responseLength);

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
            return JsonSerializer.Deserialize<ClientResponse>(jsonResponse, options);
        }
        catch (Exception)
        {
            // Handle exceptions (e.g., connection lost)
            return null;
        }
    }
}
