using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.MoqQueries;

public class GetAllLogsQuery(LogManager logManager, ICellManager cellManager)
{
    public async Task<Result<List<string>>> Execute(string cellName, string tableName)
    {
        var columnsResult = await cellManager.GetColumnsFromTable(cellName, tableName);
        if (!columnsResult.TryGetValue(out var columns)) return columnsResult.Error;
        
        var logs = logManager.GetAllLogsForCellTable(cellName, tableName);
        
        List<string> result = new();
        await foreach (var log in logs)
        {
            var deserializerResult = RowSerializer.Deserialize(log.TupleData, columns);
            if(!deserializerResult.TryGetValue(out var deserializedRow) || deserializedRow.Values.Length == 0) continue;
            
            result.Add(((string)deserializedRow.Values[1]!));
        }
        
        return result;
    }
}