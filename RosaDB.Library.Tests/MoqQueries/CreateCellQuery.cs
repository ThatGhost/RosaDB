using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.MoqQueries;

public class CreateCellQuery(IDatabaseManager databaseManager)
{
    public async Task<Result> Execute(string cellName, List<Column>? columns = null)
    {
        if (columns == null)
        {
            var columnResult = Column.Create("Id", DataType.BIGINT, isPrimaryKey: true, isIndex: true);
            if(!columnResult.TryGetValue(out var column)) return columnResult.Error;
            columns = [column];
        }
        return await databaseManager.CreateCell(cellName, columns.ToArray());
    }
}
