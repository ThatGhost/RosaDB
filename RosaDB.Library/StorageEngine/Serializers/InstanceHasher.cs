using System.Security.Cryptography;
using System.Text;

namespace RosaDB.Library.StorageEngine.Serializers;

public static class InstanceHasher
{
    public static TableInstanceIdentifier CreateIdentifier(string contextName, string tableName, object?[] indexValues)
    {
        var indexString = string.Join(";", indexValues.Select(v => v?.ToString()));
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(indexString)));
        return new TableInstanceIdentifier(contextName, tableName, hash);
    }
    
    public static string GenerateContextInstanceHash(IReadOnlyDictionary<string, string> indexValues)
    {
        var combinedIndex = string.Join("::", indexValues.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(combinedIndex)));
    }
}