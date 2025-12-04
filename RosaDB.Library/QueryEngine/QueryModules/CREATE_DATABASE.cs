using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.QueryEngine.QueryModules;

public class CREATE_DATABASE(string[] parts) : QueryModule
{
    public override async Task<Result> Execute(CancellationToken ct)
    {
        string databaseName = parts[0];
        if(string.IsNullOrEmpty(databaseName)) return new Error(ErrorPrefixes.DatatypeError, "Database name not specified");
        
        var databaseModel = new Database(databaseName, []);
        
        await FolderManager.CreateFolder(databaseName);
        await ByteReaderWriter.WriteBytesToFile(
            Path.Combine(databaseName, ".env"), 
            ByteObjectConverter.ObjectToByteArray(databaseModel), 
            ct
        );

        return Result.Success();
    }
}