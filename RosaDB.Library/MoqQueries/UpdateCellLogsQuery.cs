using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.MoqQueries
{
    public class UpdateCellLogsQuery(LogManager logManager, ICellManager cellManager)
    {
        public async Task Execute(string cellName, string tableName, object[] index, string data)
        {
            var columnResult = await cellManager.GetColumnsFromTable(cellName, tableName);
            if(columnResult.IsFailure) return;
            var logs = logManager.GetAllLogsForCellInstanceTable(cellName, tableName, index);

            int i = 0;
            await foreach (var log in logs)
            {
                var deserializeResult = RowSerializer.Deserialize(log.TupleData, columnResult.Value);
                if(deserializeResult.IsFailure || deserializeResult.Value.Values.Length != 3) continue;
                
                object?[] rowValues = { deserializeResult.Value.Values[0], $"{data}", deserializeResult.Value.Values[2] };
                var row = Row.Create(rowValues, columnResult.Value);
                if(row.IsFailure) return;

                var bytes = RowSerializer.Serialize(row.Value);
                if(bytes.IsFailure) return;
                
                logManager.Put(cellName, tableName, index, bytes.Value, log.Id);
                i++;
            }

            await logManager.Commit();
        }
    }
}
