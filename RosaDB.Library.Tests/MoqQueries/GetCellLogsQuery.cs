using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.MoqQueries;

public class GetCellLogsQuery(ILogManager logManager, ICellManager cellManager)
{
    public async Task<List<string>> Execute(string cellName, string tableName, object[] indexValues)
    {
        var logs = logManager.GetAllLogsForCellInstanceTable(cellName, tableName, indexValues);

        var cellFromDb = await cellManager.GetColumnsFromTable(cellName, tableName);
        if(cellFromDb.IsFailure) return [];

        List<string> data = new List<string>();
        await foreach (var log in logs)
        {
            var rowResult = RowSerializer.Deserialize(log.TupleData, cellFromDb.Value);
            if(!rowResult.TryGetValue(out var row)) continue;
            if (row.Values[1] is string strValue)
            {
                data.Add(strValue);
            }
        }
        
        return data;
    }
}