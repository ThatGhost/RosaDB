using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class AlterQuery(string[] tokens, IModuleManager cellManager) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (tokens.Length < 2) return new Error(ErrorPrefixes.QueryParsingError, "Invalid ALTER statement.");

        switch (tokens[1].ToUpperInvariant())
        {
            case "MODULE":
                return await ExecuteAlterModule();
            case "TABLE":
                return new Error(ErrorPrefixes.QueryParsingError, "ALTER TABLE is not implemented.");
            default:
                return new Error(ErrorPrefixes.QueryParsingError, "Unsupported ALTER statement. Expected MODULE or TABLE.");
        }
    }

    private async Task<QueryResult> ExecuteAlterModule()
    {
        if (tokens.Length < 5) return new Error(ErrorPrefixes.QueryParsingError, "Invalid ALTER MODULE syntax.");
        
        switch (tokens[3].ToUpperInvariant())
        {
            case "ADD":
                return await ExecuteAddColumn();
            case "DROP":
                return await ExecuteDropColumn();
            case "UPDATE":
                return new Error(ErrorPrefixes.QueryParsingError, "UPDATE COLUMN is not yet implemented.");
            default:
                return new Error(ErrorPrefixes.QueryParsingError, $"Unsupported action. Expected ADD, DROP, or UPDATE.");
        }
    }
    
    private async Task<QueryResult> ExecuteAddColumn()
    {
        // Expecting: ALTER MODULE <moduleName> ADD COLUMN <colName> <colType>
        if (tokens.Length != 7 || tokens[4].ToUpperInvariant() != "COLUMN")
            return new Error(ErrorPrefixes.QueryParsingError, "Invalid syntax. Expected: ALTER MODULE <moduleName> ADD COLUMN <colName> <colType>");

        var moduleName = tokens[2];
        var columnName = tokens[5];
        var columnTypeStr = tokens[6];

        if (!Enum.TryParse<DataType>(columnTypeStr, true, out var columnType)) return new Error(ErrorPrefixes.QueryParsingError, $"Invalid data type '{columnTypeStr}'.");

        var newColumn = Column.Create(columnName, columnType, false);
        if (newColumn.IsFailure) return newColumn.Error;

        var getEnvResult = await cellManager.GetEnvironment(moduleName);
        if (!getEnvResult.TryGetValue(out var env)) return getEnvResult.Error;

        var currentColumns = env.Columns.ToList();
        if (currentColumns.Any(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase)))
            return new Error(ErrorPrefixes.QueryParsingError, $"Column '{columnName}' already exists in module '{moduleName}'.");
        
        currentColumns.Add(newColumn.Value);
        
        var result = await cellManager.UpdateModuleEnvironment(moduleName, currentColumns.ToArray());
        return result.IsFailure ? result.Error : new QueryResult($"Successfully added column {columnName} to module {moduleName}.");
    }

    private async Task<QueryResult> ExecuteDropColumn()
    {
        // Expecting: ALTER MODULE <moduleName> DROP COLUMN <colName>
        if (tokens.Length != 6 || tokens[4].ToUpperInvariant() != "COLUMN")
            return new Error(ErrorPrefixes.QueryParsingError, "Invalid syntax. Expected: ALTER MODULE <moduleName> DROP COLUMN <colName>");

        var moduleName = tokens[2];
        var columnName = tokens[5];

        var result = await cellManager.DropColumnAsync(moduleName, columnName);

        return result.IsFailure ? result.Error : new QueryResult($"Successfully dropped column {columnName} from module {moduleName}.");
    }
}