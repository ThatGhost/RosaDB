namespace RosaDB.Library.Query.TokenParsers;

public static class TokensToContextAndTableParser
{
    public static (string contextName, string tableName) TokensToContextAndName(string token)
    {
        var tableNameParts = token.Split('.');
        var contextName = tableNameParts[0];
        var tableName = tableNameParts[1];
        return (contextName, tableName);
    }
}