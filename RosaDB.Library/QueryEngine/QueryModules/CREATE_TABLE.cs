using System.Text.RegularExpressions;
using RosaDB.Library.Core;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.QueryEngine.QueryModules;

public class CREATE_TABLE(string[] parts) : QueryModule
{
    private record DataDefinition(string Name, string dataType, string encapsulation, string key);
    public override Task<Result> Execute(CancellationToken ct)
    {
        // Validate and parse
        string tableName = parts[0];
        
        string fullDataDefinition = string.Join("", parts.Skip(1)).Trim();
        string[] dataDefinitionsString = fullDataDefinition.Split(',');
        List<DataDefinition> dataDefinitions = [];
        
        var regex = new Regex(@"\s*(\w+)\s+([A-Z]+(?:\(\d+(?:,\s*\d+)?\))?)\s*(PRIMARY KEY)?\s*,?");
        
        foreach (string dataDefinition in dataDefinitionsString)
        {
            var match = regex.Match(dataDefinition);
            if (match.Success)
            {
                string name = match.Groups[1].Value;
                string dataType = match.Groups[2].Value;
                string key = match.Groups[3].Value;
                dataDefinitions.Add(new DataDefinition(name, dataType, "", key));
            }
        }
        
        // Create
        // await TableManager.CreateTable(tableName, dataDefinitions);

        return Task.FromResult(Result.Success());
    }
}