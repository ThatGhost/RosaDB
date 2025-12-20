using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries
{
    public class UpdateCellLogsQuery(LogManager logManager, CellManager cellManager)
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
                Row row = new Row(rowValues, columnResult.Value);

                byte[] bytes = RowSerializer.Serialize(row);

                logManager.Put(cellName, tableName, index, bytes, log.Id);
                i++;
            }

            await logManager.Commit();
        }
    }
}
