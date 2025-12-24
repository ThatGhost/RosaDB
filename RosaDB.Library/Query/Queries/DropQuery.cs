using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.Query.Queries;
public class DropQuery(
    string[] tokens,
    RootManager rootManager,
    DatabaseManager databaseManager,
    CellManager cellManager) : IQuery
{
    public async Task<QueryResult> Execute()
    {
        if (tokens[0].ToUpperInvariant() != "DROP") return new Error(ErrorPrefixes.QueryParsingError, "Invalid query type");

        switch (tokens[1].ToUpperInvariant())
        {
            case "DATABASE": return await DROP_DATABASE(tokens[2]);
            case "CELL": return await DROP_CELL(tokens[2]);
            case "TABLE": return await DROP_TABLE(tokens[2..]);
            default: return new Error(ErrorPrefixes.QueryParsingError, "DROP type not supported");
        }
    }

    private async Task<QueryResult> DROP_DATABASE(string name)
    {
        var result = await rootManager.DeleteDatabase(name);
        if (result.IsFailure) return result.Error;

        return new QueryResult($"Database: {name} was deleted succesfully");
    }

    private async Task<QueryResult> DROP_CELL(string name)
    {
        var result = await databaseManager.DeleteCell(name);
        if (result.IsFailure) return result.Error;

        return new QueryResult($"Cell: {name} was deleted succesfully");
    }

    private async Task<QueryResult> DROP_TABLE(string[] tokens)
    {
        string tableName = tokens[0];
        if (tokens[1] != "IN") return new Error(ErrorPrefixes.QueryParsingError, "Delete table does not define IN structure");
        string cellName = tokens[2];

        var result = await cellManager.DeleteTable(cellName, tableName);
        if(result.IsFailure) return result.Error;

        return new QueryResult($"Table with name: {tableName} in cell: {cellName} was succesfully dropped");
    }
}
