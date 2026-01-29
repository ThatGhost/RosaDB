using System.Security.Cryptography;
using System.Text;

namespace RosaDB.Library.StorageEngine.Serializers;

public static class InstanceHasher
{
    public static TableInstanceIdentifier CreateIdentifier(string moduleName, string tableName, string instanceHash)
    {
        return new TableInstanceIdentifier(moduleName, tableName, instanceHash);
    }
    
    public static string GenerateModuleInstanceHash(Dictionary<string, string> indexValues)
    {
        var combinedIndex = string.Join("::", indexValues.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(combinedIndex)));
    }
    
    public static string GenerateModuleInstanceHash(IReadOnlyDictionary<string, string> indexValues)
    {
        var combinedIndex = string.Join("::", indexValues.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(combinedIndex)));
    }
}