
using System.Text.Json;

namespace RosaDB.Library.StorageEngine;

public class ByteObjectConverter
{
    public static byte[] ObjectToByteArray(object obj)
    {
        return JsonSerializer.SerializeToUtf8Bytes(obj);
    }
    
    public static T? ByteArrayToObject<T>(byte[] bytes)
    {
        return JsonSerializer.Deserialize<T>(bytes);
    }
}