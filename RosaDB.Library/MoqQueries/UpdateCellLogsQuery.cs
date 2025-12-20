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
                object?[] rowValues = { $"{data} - {i}" };
                Row row = new Row(rowValues, columnResult.Value);

                byte[] bytes = RowSerializer.Serialize(row);

                logManager.Put(cellName, tableName, index, bytes, log.Id);
                i++;
            }

            await logManager.Commit();
        }
    }
}
