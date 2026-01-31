using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using System.IO.Abstractions;
using System.Text.Json;

namespace RosaDB.Library.StorageEngine;

public class DatabaseManager(
    SessionState sessionState, 
    IFileSystem fileSystem, 
    IFolderManager folderManager,
    IModuleManager moduleManager) : IDatabaseManager
{
    public async Task<Result<Database>> GetDatabase(string databaseName)
    {
        if(!folderManager.DoesFolderExist(databaseName)) return new Error(ErrorPrefixes.FileError, "Database not found");
        string databaseEnvPath = GetDatabaseEnvPath(databaseName);
        if(!fileSystem.File.Exists(databaseEnvPath)) return new Error(ErrorPrefixes.FileError, "Database not found");

        try
        {
            await using FileStream fs = File.Open(databaseEnvPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var database = await JsonSerializer.DeserializeAsync<Database>(fs);
            return database is null ? new Error(ErrorPrefixes.DataError, "Could not read the database format") : database;
        }
        catch {  return new Error(ErrorPrefixes.DataError, "Could not read the database format"); }
    }

    public async Task<Result> CreateDatabase(Database database)
    {
        if (!folderManager.DoesFolderExist(database.Name)) folderManager.CreateFolder(database.Name);
        string databaseEnvPath = GetDatabaseEnvPath(database.Name);
        if (fileSystem.File.Exists(databaseEnvPath)) return new Error(ErrorPrefixes.FileError, "Database already exists");

        await SaveDatabase(database);
        
        // Every database comes with a default module
        await CreateModule(Module.Create(IDatabaseManager.DefaultModuleName, []).Value!);
        return await moduleManager.InsertModuleInstance(IDatabaseManager.DefaultModuleName, Row.Create([],[]).Value!);
    }

    public Task<Result> DeleteDatabase(string name)
    {
        folderManager.DeleteFolder(name);
        return Task.FromResult(Result.Success());
    }

    public async Task<Result<Module>> GetModule(string moduleName)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        var databaseResult = await GetDatabase(sessionState.CurrentDatabase.Name);
        if (!databaseResult.TryGetValue(out var database)) return databaseResult.Error;

        var module = database.GetModule(moduleName);
        if (module is null) return new Error(ErrorPrefixes.DataError, "Module does not exists");
        return module;
    }
    
    public async Task<Result> CreateModule(Module module)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        var databaseResult = await GetDatabase(sessionState.CurrentDatabase.Name);
        if (!databaseResult.TryGetValue(out var database)) return databaseResult.Error;

        if (database.Modules.Any(m => m.Name == module.Name)) return new Error(ErrorPrefixes.DataError, "Module already exists");
        database.Modules.Add(module);
        
        return await SaveDatabase(database);
    }
    
    public async Task<Result> DeleteModule(string name)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        var databaseResult = await GetDatabase(sessionState.CurrentDatabase.Name);
        if (!databaseResult.TryGetValue(out var database)) return databaseResult.Error;

        if (database.Modules.All(m => m.Name != name)) return new Error(ErrorPrefixes.DataError, "Module does not exists");
        database.Modules.RemoveAll(m => m.Name == name);
        
        return await SaveDatabase(database);
    }

    public async Task<Result> CreateTable(string moduleName, Table table)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        var databaseResult = await GetDatabase(sessionState.CurrentDatabase.Name);
        if (!databaseResult.TryGetValue(out var database)) return databaseResult.Error;

        var module = database.Modules.FirstOrDefault(m => m.Name == moduleName);
        if (module is null) return new Error(ErrorPrefixes.DataError, "Module does not exists");
        
        module.Tables.Add(table);
        return await SaveDatabase(database);
    }

    public async Task<Result> DeleteTable(string moduleName, string tableName)
    {
        if (sessionState.CurrentDatabase is null) return new DatabaseNotSetError();
        var databaseResult = await GetDatabase(sessionState.CurrentDatabase.Name);
        if (!databaseResult.TryGetValue(out var database)) return databaseResult.Error;

        var module = database.GetModule(moduleName);
        if (module is null) return new Error(ErrorPrefixes.DataError, "Module does not exists");
        
        if (database.Modules.All(m => m.Name != tableName)) return new Error(ErrorPrefixes.DataError, "table does not exists");
        module.Tables.RemoveAll(m => m.Name == tableName);
        
        return await SaveDatabase(database);
    }

    public async Task<Result<Table>> GetTable(string moduleName, string tableName)
    {
        var moduleResult = await GetModule(moduleName);
        if (!moduleResult.TryGetValue(out var module)) return moduleResult.Error;

        var table = module.GetTable(tableName);
        if (table is null) return new Error(ErrorPrefixes.DataError, "table does not exists");
        return table;
    }

    private async Task<Result> SaveDatabase(Database database)
    {
        try
        {
            string databaseEnvPath = GetDatabaseEnvPath(database.Name);
            await using var fs = File.Create(databaseEnvPath);
            await JsonSerializer.SerializeAsync(fs, database);
            return Result.Success();
        }
        catch { return new Error(ErrorPrefixes.FileError, "Could not create database"); }
    }
    
    public string GetDatabaseEnvPath(string databaseName)
    {
        return fileSystem.Path.Combine(folderManager.BasePath, databaseName, "_env");
    }
}

