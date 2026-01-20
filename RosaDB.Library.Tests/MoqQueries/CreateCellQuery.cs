using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.MoqQueries;

public class CreateContextQuery(IDatabaseManager databaseManager)
{
    public async Task<Result> Execute(string contextName, List<Column>? columns = null)
    {
        if (columns == null)
        {
            var columnResult = Column.Create("Id", DataType.BIGINT, isPrimaryKey: true, isIndex: true);
            if(!columnResult.TryGetValue(out var column)) return columnResult.Error;
            columns = [column];
        }
        return await databaseManager.CreateContext(contextName, columns.ToArray());
    }
}
