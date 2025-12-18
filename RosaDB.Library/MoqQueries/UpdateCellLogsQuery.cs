using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries
{
    public class UpdateCellLogsQuery(LogManager logManager)
    {

        public async Task Execute(string cellName, string tableName, object[] index, string data)
        {
            Cell cell = new Cell(cellName);
            Table table = new Table(tableName);

            var logs = logManager.GetAllLogsForCellInstanceTable(cell, table, index);

            int i = 0;
            await foreach (var log in logs)
            {
                var payload = ($"{data} - {i}").ToCharArray();
                byte[] bytes = ByteObjectConverter.ObjectToByteArray(payload);

                logManager.Put(cell, table, index, bytes, log.Id);
                i++;
            }

            await logManager.Commit();
        }
    }
}
