using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.MoqQueries
{
    public class UpdateCellLogsQuery(ILogManager logManager, ICellManager cellManager)
    {
        public async Task<Result> Execute(string cellName, string tableName, object[] index, string data)
        {
            var columnResult = await cellManager.GetColumnsFromTable(cellName, tableName);
            if(!columnResult.TryGetValue(out var columns)) return columnResult.Error;
            var logs = logManager.GetAllLogsForCellInstanceTable(cellName, tableName, index);

            int i = 0;
            await foreach (var log in logs)
            {
                var deserializeResult = RowSerializer.Deserialize(log.TupleData, columns);
                if(!deserializeResult.TryGetValue(out var deserializedRow)|| deserializedRow.Values.Length != 3) continue;
                
                object?[] rowValues = { deserializedRow.Values[0], $"{data}", deserializedRow.Values[2] };
                var rowResult = Row.Create(rowValues, columns);
                if(!rowResult.TryGetValue(out var row)) return rowResult.Error;

                var bytesResult = RowSerializer.Serialize(row);
                if(!bytesResult.TryGetValue(out var bytes)) return bytesResult.Error;
                
                logManager.Put(cellName, tableName, index, bytes, logId: log.Id);
                i++;
            }

            await logManager.Commit();
            return Result.Success();
        }
    }
}
