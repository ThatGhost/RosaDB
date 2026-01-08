using System.Security.Cryptography;
using System.Text;

namespace RosaDB.Library.StorageEngine.Serializers;

public static class InstanceHasher
{
    public static TableInstanceIdentifier CreateIdentifier(string cellName, string tableName, object?[] indexValues)
    {
        var indexString = string.Join(";", indexValues.Select(v => v?.ToString()));
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(indexString)));
        return new TableInstanceIdentifier(cellName, tableName, hash);
    }
    
    public static string GenerateCellInstanceHash(IReadOnlyDictionary<string, string> indexValues)
    {
        var combinedIndex = string.Join("::", indexValues.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(combinedIndex)));
    }
}