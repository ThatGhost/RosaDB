using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace RosaDB.Library.StorageEngine;

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

    public static T? ReadObjectFromStream<T>(Stream stream)
    {
        var lengthBytes = new byte[4];
        int bytesRead = stream.Read(lengthBytes, 0, 4);
        if (bytesRead < 4) return default;

        if (!BitConverter.IsLittleEndian) Array.Reverse(lengthBytes);

        var length = BitConverter.ToInt32(lengthBytes, 0);

        if (stream.Length - stream.Position < length) return default;

        var objectBytes = new byte[length];
        _ = stream.Read(objectBytes, 0, length);
        
        return JsonSerializer.Deserialize<T>(objectBytes);
    }

    public static async Task<T?> ReadObjectFromStreamAsync<T>(Stream stream, CancellationToken ct = default)
    {
        var lengthBytes = new byte[4];
        int bytesRead = await stream.ReadAsync(lengthBytes, 0, 4, ct);
        if (bytesRead < 4) return default;

        if (!BitConverter.IsLittleEndian) Array.Reverse(lengthBytes);

        var length = BitConverter.ToInt32(lengthBytes, 0);
        
        if (stream.CanSeek && (stream.Length - stream.Position < length)) return default;

        var objectBytes = new byte[length];
        int totalRead = 0;
        while (totalRead < length)
        {
            int read = await stream.ReadAsync(objectBytes, totalRead, length - totalRead, ct);
            if (read == 0) return default; // Unexpected EOF
            totalRead += read;
        }
        
        using var ms = new MemoryStream(objectBytes);
        return await JsonSerializer.DeserializeAsync<T>(ms, cancellationToken: ct);
    }
}