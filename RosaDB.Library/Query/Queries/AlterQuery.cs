using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class AlterQuery(string[] tokens, ICellManager cellManager) : IQuery
{
    public async ValueTask<QueryResult> Execute()
    {
        if (tokens.Length < 2) return new Error(ErrorPrefixes.QueryParsingError, "Invalid ALTER statement.");

        switch (tokens[1].ToUpperInvariant())
        {
            case "CELL":
                return await ExecuteAlterCell();
            case "TABLE":
                return new Error(ErrorPrefixes.QueryParsingError, "ALTER TABLE is not implemented.");
            default:
                return new Error(ErrorPrefixes.QueryParsingError, "Unsupported ALTER statement. Expected CELL or TABLE.");
        }
    }

    private async Task<QueryResult> ExecuteAlterCell()
    {
        if (tokens.Length < 5) return new Error(ErrorPrefixes.QueryParsingError, "Invalid ALTER CELL syntax.");
        
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
        // Expecting: ALTER CELL <cellName> ADD COLUMN <colName> <colType>
        if (tokens.Length != 7 || tokens[4].ToUpperInvariant() != "COLUMN")
            return new Error(ErrorPrefixes.QueryParsingError, "Invalid syntax. Expected: ALTER CELL <cellName> ADD COLUMN <colName> <colType>");

        var cellName = tokens[2];
        var columnName = tokens[5];
        var columnTypeStr = tokens[6];

        if (!Enum.TryParse<DataType>(columnTypeStr, true, out var columnType)) return new Error(ErrorPrefixes.QueryParsingError, $"Invalid data type '{columnTypeStr}'.");

        var newColumn = Column.Create(columnName, columnType, false);
        if (newColumn.IsFailure) return newColumn.Error;

        var getEnvResult = await cellManager.GetEnvironment(cellName);
        if (!getEnvResult.TryGetValue(out var env)) return getEnvResult.Error;

        var currentColumns = env.Columns.ToList();
        if (currentColumns.Any(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase)))
            return new Error(ErrorPrefixes.QueryParsingError, $"Column '{columnName}' already exists in cell '{cellName}'.");
        
        currentColumns.Add(newColumn.Value);
        
        var result = await cellManager.UpdateCellEnvironment(cellName, currentColumns.ToArray());
        return result.IsFailure ? result.Error : new QueryResult($"Successfully added column {columnName} to cell {cellName}.");
    }

    private async Task<QueryResult> ExecuteDropColumn()
    {
        // Expecting: ALTER CELL <cellName> DROP COLUMN <colName>
        if (tokens.Length != 6 || tokens[4].ToUpperInvariant() != "COLUMN")
            return new Error(ErrorPrefixes.QueryParsingError, "Invalid syntax. Expected: ALTER CELL <cellName> DROP COLUMN <colName>");

        var cellName = tokens[2];
        var columnName = tokens[5];

        var result = await cellManager.DropColumnAsync(cellName, columnName);

        return result.IsFailure ? result.Error : new QueryResult($"Successfully dropped column {columnName} from cell {cellName}.");
    }
}