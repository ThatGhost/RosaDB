using CSharpTest.Net.IO;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using RosaDB.Library.Websockets.Interfaces;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace RosaDB.Library.Websockets;

public class SubscriptionManager(ICellManager cellManager) : ISubscriptionManager
{
    private readonly Dictionary<TableInstanceIdentifier, List<WebSocket>> _subscriptions = new();

    public async Task<QueryResult> HandleSubscribe(string[] tokens, WebSocket webSocket)
    {
        if(tokens.Length < 6) return new Error(ErrorPrefixes.QueryParsingError, "Could not parse SUBSCRIBE action. Correct format is 'SUBSCRIBE <cell>.<table> USING <column> = <value>'");
        if (tokens[0].ToUpperInvariant() != "SUBSCRIBE") return new Error(ErrorPrefixes.QueryParsingError, "Could not parse this query because query type is incorrect");
        string[] nameParts = tokens[1].Split(".");
        if (nameParts.Length != 2) return new Error(ErrorPrefixes.QueryParsingError, "Could not parse cell instance name. Correct format is '<cell>.<table>'");

        string cellName = nameParts[0];
        string tableName = nameParts[1];

        if (!tokens[2].Equals("USING", StringComparison.InvariantCultureIgnoreCase)) return new Error(ErrorPrefixes.QueryParsingError, "Could not find USING statement at correct position. Correct statement is 'SUBSCRIBE <cell> USING <clause>'");

        var cellInstanceResult = await GetCellInstance(cellName, tokens);
        if (cellInstanceResult.IsFailure) return cellInstanceResult.Error;

        TableInstanceIdentifier tableIdentifier = new TableInstanceIdentifier(cellName, tableName, cellInstanceResult.Value);
        try
        {
            if (!_subscriptions.ContainsKey(tableIdentifier))
            {
                _subscriptions[tableIdentifier] = new List<WebSocket>();
            }
            _subscriptions[tableIdentifier].Add(webSocket);
        }
        catch
        {
            return new Error(ErrorPrefixes.StateError, $"Failed to subscribe to {cellName}. You might already be subscribed.");
        }

        return new QueryResult($"Successfully subscribed to {cellName}.{tableName} instance");
    }

    private Result<Dictionary<string, string>> ParseUsingClause(int startIndex, string[] tokens)
    {
        var usingClauseTokens = tokens[startIndex..];
        if (usingClauseTokens[^1] == ";") usingClauseTokens = usingClauseTokens[..^1];

        var usingProperties = new Dictionary<string, string>();
        for (int i = 0; i < usingClauseTokens.Length; i += 3)
        {
            if (i + 2 >= usingClauseTokens.Length || usingClauseTokens[i + 1] != "=") return new Error(ErrorPrefixes.QueryParsingError, "Malformed USING clause. Expected 'key=value' pairs.");

            var value = usingClauseTokens[i + 2];
            if (value.StartsWith("'") && value.EndsWith("'"))
                value = value.Substring(1, value.Length - 2);

            usingProperties[usingClauseTokens[i]] = value;
        }

        return usingProperties;
    }

    private async Task<Result<string>> GetCellInstance(string cellName, string[] tokens)
    {
        var cellEnvResult = await cellManager.GetEnvironment(cellName);
        if (!cellEnvResult.TryGetValue(out var cellEnv)) return cellEnvResult.Error;

        var parsed = ParseUsingClause(3, tokens);
        if (parsed.IsFailure) return parsed.Error;

        var cellSchemaColumns = cellEnv.Columns;
        var usingIndexValues = new Dictionary<string, string>();

        foreach (var kvp in parsed.Value)
        {
            var col = cellSchemaColumns.FirstOrDefault(c => c.Name.Equals(kvp.Key, StringComparison.OrdinalIgnoreCase));
            if (col == null) return new Error(ErrorPrefixes.DataError, $"Property '{kvp.Key}' in USING clause does not exist in CellGroup '{cellName}'.");
            if (!col.IsIndex) return new Error(ErrorPrefixes.DataError, $"Property '{kvp.Key}' in USING clause is not an indexed column for CellGroup '{cellName}'.");

            usingIndexValues[col.Name] = kvp.Value;
        }

        if (usingIndexValues.Count != cellSchemaColumns.Count(c => c.IsIndex)) return new Error(ErrorPrefixes.DataError, $"USING clause requires values for all indexed CellGroup columns.");

        var cellInstanceHash = InstanceHasher.GenerateCellInstanceHash(usingIndexValues);

        var cellInstanceResult = await cellManager.GetCellInstance(cellName, cellInstanceHash);
        if (cellInstanceResult.IsFailure) return cellInstanceResult.Error;

        return cellInstanceHash;
    }

    public async Task<QueryResult> HandleUnsubscribe(string[] tokens, WebSocket webSocket)
    {
        if (tokens[0].ToUpperInvariant() != "UNSUBSCRIBE") return new Error(ErrorPrefixes.QueryParsingError, "Could not parse this query because query type is incorrect");
        string[] nameParts = tokens[1].Split(".");
        if (nameParts.Length != 2) return new Error(ErrorPrefixes.QueryParsingError, "Could not parse cell instance name. Correct format is '<cell>.<table>'");

        string cellName = nameParts[0];
        string tableName = nameParts[1];

        if (tokens[2].ToUpperInvariant() != "USING") return new Error(ErrorPrefixes.QueryParsingError, "Could not find USING statement at correct position. Correct statement is 'UNSUBSCRIBE <cell> USING <clause>'");

        var cellInstanceResult = await GetCellInstance(cellName, tokens);
        if (cellInstanceResult.IsFailure) return cellInstanceResult.Error;
        
        TableInstanceIdentifier tableIdentifier = new TableInstanceIdentifier(cellName, tableName, cellInstanceResult.Value);
        if (!_subscriptions.ContainsKey(tableIdentifier)) return new Error(ErrorPrefixes.StateError, $"Failed to unsubscribe from {cellName}. You might not be subscribed.");
        if (!_subscriptions[tableIdentifier].Remove(webSocket)) return new Error(ErrorPrefixes.StateError, $"Failed to unsubscribe from {cellName}. You might not be subscribed.");

        return new QueryResult($"Succesfully unsubscribed to {nameParts} instance");
    }

    public void RemoveWebSocket(WebSocket webSocket)
    {
        foreach (var subscriptionList in _subscriptions.Values)
        {
            subscriptionList.Remove(webSocket);
        }
    }

    private readonly JsonSerializerOptions _jsonOptions = new(){ WriteIndented = false };
    public async Task NotifySubscriber(TableInstanceIdentifier tableIdentifier, Row newRow)
    {
        if (!_subscriptions.TryGetValue(tableIdentifier, out var webSockets)) return;
        if (webSockets.Count == 0) return;
        
        var rowDict = new Dictionary<string, object?>();
        for (int i = 0; i < newRow.Values.Length; i++) rowDict[newRow.Columns[i].Name] = newRow.Values[i];

        var jsonPayload = JsonSerializer.Serialize(rowDict, _jsonOptions);
        var jsonBytes = Encoding.UTF8.GetBytes(jsonPayload);
        var lengthBytes = BitConverter.GetBytes(jsonBytes.Length);

        foreach (var socket in webSockets)
        {
            if (socket.State != WebSocketState.Open) continue;
            
            await socket.SendAsync(lengthBytes, WebSocketMessageType.Binary, true, CancellationToken.None);
            await socket.SendAsync(jsonBytes, WebSocketMessageType.Binary, true, CancellationToken.None);
        }
    }
}
