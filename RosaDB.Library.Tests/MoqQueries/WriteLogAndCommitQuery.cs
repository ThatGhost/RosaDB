using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Serializers;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.Core;

namespace RosaDB.Library.MoqQueries;

public class WriteLogAndCommitQuery(ILogWriter logWriter, IContextManager cellManager)
{
    public async Task<Result> Execute(string context, string table, string data)
    {
        var columnsResult = await cellManager.GetColumnsFromTable(context, table);
        if (!columnsResult.TryGetValue(out var columns)) return columnsResult.Error;

        var random = new Random();

        for (int i = 0; i < 10000; i++)
        {
            object?[] rowValues = [i, $"{data}", random.Next(10, 100)];
            var rowResult = Row.Create(rowValues, columns);
            if(!rowResult.TryGetValue(out var row)) return rowResult.Error;
            
            var bytesResult = RowSerializer.Serialize(row);
            if(!bytesResult.TryGetValue(out var bytes)) return bytesResult.Error;
            
            logWriter.Put(context, table, [random.Next(0,4)], bytes);
        }

        await logWriter.Commit();
        return Result.Success();
    }
}

