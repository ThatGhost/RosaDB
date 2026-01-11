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
        // Expecting: ALTER CELL <cellName> ADD COLUMN <colName> <colType>
        if (tokens.Length != 7 || tokens[3].ToUpperInvariant() != "ADD" || tokens[4].ToUpperInvariant() != "COLUMN")
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
        return result.IsFailure ? result.Error : new QueryResult($"Successfully updated cell {cellName}.");
    }
}