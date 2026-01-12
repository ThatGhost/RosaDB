using LightInject;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query;
using RosaDB.Library.Query.Queries;
using RosaDB.Library.Server.Interfaces;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace RosaDB.Library.Server;

public class ClientSession
{
    private readonly TcpClient _client;
    private readonly Scope _scope;
    private readonly ISystemLogPublisher _logPublisher;

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

    public ClientSession(TcpClient client, Scope scope, ISystemLogPublisher logPublisher)
    {
        _client = client;
        _scope = scope;
        _logPublisher = logPublisher;
    }

    public async Task HandleClient()
    {
        var queryTokenizer = _scope.GetInstance<QueryTokenizer>();
        var queryPlanner = _scope.GetInstance<QueryPlanner>();
        var sessionId = InitializeClientSession(_scope);
        
        await using var stream = _client.GetStream();
        await SendResponseAsync(stream, new QueryResult($"Session initialized with ID: {sessionId}"), TimeSpan.Zero);
        while (_client.Connected)
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
                _logPublisher.Publish(RosaDB.Library.Server.Interfaces.LogLevel.Information, $"Executing query: {query}");
                QueryResult result = await ExecuteQueries(query, queryTokenizer, queryPlanner);

                DateTime end = DateTime.Now;
                TimeSpan duration = end - start;

                await SendResponseAsync(stream, result, duration);
            }
            catch (IOException)
            {
                _logPublisher.Publish(RosaDB.Library.Server.Interfaces.LogLevel.Error, "An IO exception occurred in the client session.");
                await SendResponseAsync(stream, new CriticalError(), TimeSpan.FromSeconds(1));
            }
            catch (Exception ex)
            {
                _logPublisher.Publish(RosaDB.Library.Server.Interfaces.LogLevel.Critical, $"An unhandled exception occurred: {ex.Message}");
                await SendResponseAsync(stream, new CriticalError(), TimeSpan.FromSeconds(1));
            }
        }
    }

    private async Task SendResponseAsync(NetworkStream stream, QueryResult result, TimeSpan duration)
    {
        if (result.IsStreaming)
        {
            // Streaming response
            int rowsAffected = 0;
            await foreach(var row in result.RowStream)
            {
                var rowDict = new Dictionary<string, object?>();
                for (int i = 0; i < row.Columns.Length; i++)
                {
                    rowDict[row.Columns[i].Name] = row.Values[i];
                }

                var responseDto = new ClientResponse
                {
                    Message = "Row stream",
                    Rows = [rowDict],
                    DurationMs = 0
                };

                var jsonPayload = JsonSerializer.Serialize(responseDto, _jsonOptions);
                var jsonBytes = Encoding.UTF8.GetBytes(jsonPayload);

                var lengthBytes = BitConverter.GetBytes(jsonBytes.Length);
                await stream.WriteAsync(lengthBytes);
                await stream.WriteAsync(jsonBytes);
                rowsAffected++;
            }

            // Send end-of-stream message
            var endOfStreamDto = new ClientResponse
            {
                Message = "Got rows successfully",
                RowsAffected = rowsAffected,
                DurationMs = duration.TotalMilliseconds
            };
            var endJsonPayload = JsonSerializer.Serialize(endOfStreamDto, _jsonOptions);
            var endJsonBytes = Encoding.UTF8.GetBytes(endJsonPayload);
            var endLengthBytes = BitConverter.GetBytes(endJsonBytes.Length);
            await stream.WriteAsync(endLengthBytes);
            await stream.WriteAsync(endJsonBytes);
        }
        else
        {
            // Non-streaming response
            var responseDto = new ClientResponse
            {
                Message = result.Message,
                RowsAffected = result.RowsAffected,
                DurationMs = duration.TotalMilliseconds,
                Rows = null
            };

            if (result.Rows.Count > 0)
            {
                responseDto.Rows = new List<Dictionary<string, object?>>();
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
    }

    private async Task<QueryResult> ExecuteQueries(string query, QueryTokenizer queryTokenizer, QueryPlanner queryPlanner)
    {
        var tokenListsResult = queryTokenizer.Tokenize(query);
        if (!tokenListsResult.TryGetValue(out var tokenLists)) return tokenListsResult.Error;

        var queryPlansResult = queryPlanner.CreateQueryPlans(tokenLists);
        if (!queryPlansResult.TryGetValue(out var queryPlans)) return queryPlansResult.Error;
        
        QueryResult lastResult = new QueryResult("No queries executed.");

        foreach (var queryPlan in queryPlans)
        {
            lastResult = await queryPlan.Execute();
            
            if (lastResult.IsError) return lastResult; 
            if (queryPlan is SelectQuery)  return lastResult;
        }
        
        return lastResult;
    }

    private Guid InitializeClientSession(Scope scope)
    {
        var sessionState = scope.GetInstance<SessionState>();
        sessionState.SessionId = Guid.NewGuid();

        // Publish the session creation event
        _logPublisher.Publish(RosaDB.Library.Server.Interfaces.LogLevel.Information, $"New client session initialized.");

        return sessionState.SessionId;
    }
}
