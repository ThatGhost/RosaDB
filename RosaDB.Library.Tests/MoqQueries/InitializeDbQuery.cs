using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.MoqQueries;

public class InitializeDbQuery(RootManager rootManager)
{
    public async Task Execute()
    {
        await rootManager.InitializeRoot();
    }
}