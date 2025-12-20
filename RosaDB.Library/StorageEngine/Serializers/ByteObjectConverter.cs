using System.Text.Json;

namespace RosaDB.Library.StorageEngine.Serializers;

public static class ByteObjectConverter
{
    public static byte[] ObjectToByteArray(object obj)
    {
        var jsonData = JsonSerializer.SerializeToUtf8Bytes(obj);
        var length = jsonData.Length;
        var lengthBytes = BitConverter.GetBytes(length);

        // This check is important for cross-platform consistency.
        if (!BitConverter.IsLittleEndian) Array.Reverse(lengthBytes);

        var result = new byte[4 + length];
        lengthBytes.CopyTo(result, 0);
        jsonData.CopyTo(result, 4);

        return result;
    }
    
    public static T? ByteArrayToObject<T>(byte[] bytes)
    {
        return JsonSerializer.Deserialize<T>(bytes[4..]);
    } 
}