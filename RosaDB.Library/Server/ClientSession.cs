using System.Net.Sockets;
using System.Text;
using LightInject;
using RosaDB.Library.Models;
using RosaDB.Library.Query;
using System.Text.Json;
using RosaDB.Library.Core;

namespace RosaDB.Library.Server;

public class ClientSession(TcpClient client, Scope scope)
{
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        WriteIndented = false
    };
    
    private class ClientResponse
    {
        public string Message { get; set; } = "";
        public int RowsAffected { get; set; }
        public List<Dictionary<string, object?>>? Rows { get; set; }
        public double DurationMs { get; set; }
    }

    public async Task HandleClient()
    {
        var queryTokenizer = scope.GetInstance<QueryTokenizer>();
        var queryPlanner = scope.GetInstance<QueryPlanner>();
        await using var stream = client.GetStream();
        while (client.Connected)
        {
            try
            {
                var lengthBuffer = new byte[4];
                var bytesRead = await stream.ReadAsync(lengthBuffer, 0, lengthBuffer.Length);
                if (bytesRead < 4) break;

                var queryLength = BitConverter.ToInt32(lengthBuffer, 0);
                if (queryLength <= 0) continue;

                var queryBuffer = new byte[queryLength];
                bytesRead = await stream.ReadAsync(queryBuffer, 0, queryBuffer.Length);
                if (bytesRead < queryLength)  break;

                DateTime start = DateTime.Now;

                var query = Encoding.UTF8.GetString(queryBuffer, 0, bytesRead);
                QueryResult result = await TokensToQueryExecution(query, queryTokenizer, queryPlanner);

                DateTime end = DateTime.Now;
                TimeSpan duration = end - start;

                await SendResponseAsync(stream, result, duration);
            }
            catch (IOException)
            {
                await SendResponseAsync(stream, new CriticalError(), TimeSpan.FromSeconds(1));
            }
            catch (Exception)
            {
                await SendResponseAsync(stream, new CriticalError(), TimeSpan.FromSeconds(1));
            }
        }
    }

    private async Task SendResponseAsync(NetworkStream stream, QueryResult result, TimeSpan duration)
    {
        var responseDto = new ClientResponse
        {
            Message = result.Message,
            RowsAffected = result.RowsAffected,
            DurationMs = duration.TotalMilliseconds,
            Rows = null
        };

        if (result.Rows.Count > 0)
        {
            responseDto.Rows = [];
            foreach (var row in result.Rows)
            {
                var rowDict = new Dictionary<string, object?>();
                for (int i = 0; i < row.Columns.Length; i++)
                {
                    rowDict[row.Columns[i].Name] = row.Values[i];
                }
                responseDto.Rows.Add(rowDict);
            }
        }
        
        var jsonPayload = JsonSerializer.Serialize(responseDto, _jsonOptions);
        var jsonBytes = Encoding.UTF8.GetBytes(jsonPayload);

        var lengthBytes = BitConverter.GetBytes(jsonBytes.Length);
        await stream.WriteAsync(lengthBytes);
        await stream.WriteAsync(jsonBytes);
    }

    private async Task<QueryResult> TokensToQueryExecution(string query, QueryTokenizer queryTokenizer, QueryPlanner queryPlanner)
    {
        var tokensResult = queryTokenizer.TokenizeQuery(query);
        if (!tokensResult.TryGetValue(out var tokens)) return tokensResult.Error;

        var queryPlanResult = queryPlanner.CreateQueryPlanFromTokens(tokens);
        if (!queryPlanResult.TryGetValue(out var queryPlan)) return queryPlanResult.Error;

        return await queryPlan.Execute();
    }
}
