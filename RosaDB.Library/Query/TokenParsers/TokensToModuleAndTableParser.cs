using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Query.TokenParsers;

public static class TokensToModuleAndTableParser
{
    public static (string moduleName, string tableName) TokensToModuleAndName(string token)
    {
        var tableNameParts = token.Split('.');
        if (tableNameParts.Length != 2) return (IDatabaseManager.DefaultModuleName, token);
        
        var moduleName = tableNameParts[0];
        var tableName = tableNameParts[1];
        return (moduleName, tableName);
    }
}