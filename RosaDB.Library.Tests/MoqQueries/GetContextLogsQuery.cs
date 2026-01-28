using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Tests.MoqQueries;

public class GetContextLogsQuery(ILogReader logReader, IContextManager cellManager)
{
    public async Task<List<string>> Execute(string contextName, string tableName, object[] indexValues)
    {
        var logs = logReader.GetAllLogsForContextInstanceTable(contextName, tableName, indexValues);

        var cellFromDb = await cellManager.GetColumnsFromTable(contextName, tableName);
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