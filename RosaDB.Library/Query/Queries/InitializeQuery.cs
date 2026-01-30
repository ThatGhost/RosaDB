using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using LightInject;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.Queries;

public class InitializeQuery(IDatabaseManager databaseManager) : IQuery
{
    private const string SystemDatabaseName = "_system";
    private const string RolesModuleName = "_roles";
    private const string UsersTableName = "_users";
    private const string PermissionsTableName = "_permissions";
    
    public async ValueTask<QueryResult> Execute()
    {
        await databaseManager.CreateDatabase(Database.Create(SystemDatabaseName).Value!);
        var dbResult = await databaseManager.GetDatabase(SystemDatabaseName);
        if (dbResult.IsFailure) return new CriticalError();

        await databaseManager.CreateModule(Module.Create(RolesModuleName, [
            Column.Create("name", DataType.TEXT, isIndex: true, isNullable: false).Value!,
        ]).Value!);

        await databaseManager.CreateTable(RolesModuleName, Table.Create(UsersTableName, [
            Column.Create("userId", DataType.INT, isNullable: false).Value!
        ]).Value!);

        await databaseManager.CreateTable(PermissionsTableName, Table.Create(PermissionsTableName, [
            Column.Create("name", DataType.TEXT, isNullable: false).Value!,
            // TODO figure out permissions
        ]).Value!);
        
        await databaseManager.CreateTable(IDatabaseManager.DefaultModuleName, Table.Create(UsersTableName, [
            Column.Create("userId", DataType.INT, isPrimaryKey: true).Value!,
            Column.Create("name", DataType.TEXT, isNullable: false).Value!,
            Column.Create("last_login", DataType.DATETIME, isNullable: true).Value!,
            Column.Create("password", DataType.TEXT, isNullable: false).Value!,
        ]).Value!);
        
        return new Error(ErrorPrefixes.CriticalError, "Database initialization is not implemented.");
    }
}