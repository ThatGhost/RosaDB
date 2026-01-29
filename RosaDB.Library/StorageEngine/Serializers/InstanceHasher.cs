using System.Security.Cryptography;
using System.Text;

namespace RosaDB.Library.StorageEngine.Serializers;

public static class InstanceHasher
{
    public static TableInstanceIdentifier CreateIdentifier(string contextName, string tableName, string instanceHash)
    {
        return new TableInstanceIdentifier(contextName, tableName, instanceHash);
    }
    
    public static string GenerateContextInstanceHash(Dictionary<string, string> indexValues)
    {
        var combinedIndex = string.Join("::", indexValues.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(combinedIndex)));
    }
    
    public static string GenerateContextInstanceHash(IReadOnlyDictionary<string, string> indexValues)
    {
        var combinedIndex = string.Join("::", indexValues.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(combinedIndex)));
    }
}