using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.MoqQueries;

public class GetAllLogsQuery(LogManager logManager, CellManager cellManager)
{
    public async Task<Result<List<string>>> Execute(string cellName, string tableName)
    {
        var columns = await cellManager.GetColumnsFromTable(cellName, tableName);
        if (columns.IsFailure) return columns.Error!;
        
        var logs = logManager.GetAllLogsForCellTable(cellName, tableName);
        
        List<string> result = new();
        await foreach (var log in logs)
        {
            var deserializerResult = RowSerializer.Deserialize(log.TupleData, columns.Value);
            if(deserializerResult.IsFailure || deserializerResult.Value.Values.Length == 0) continue;
            
            result.Add(((string)deserializerResult.Value.Values[1]!)!);
        }
        
        return result;
    }
}