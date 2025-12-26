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
            var column = Column.Create("Id", DataType.BIGINT, isPrimaryKey: true, isIndex: true);
            if(column.IsFailure) return column.Error;
            columns = [column.Value];
        }
        return await databaseManager.CreateCell(cellName, columns);
    }
}
