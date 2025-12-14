using System.IO;

namespace RosaDB.Library.StorageEngine;

public static class ByteReaderWriter
{
    public static async Task<byte[]> ReadBytesFromFile(string path, CancellationToken ct)
    {
        return !File.Exists(path) ? [] : await File.ReadAllBytesAsync(path, ct);
    }

    public static async Task WriteBytesToFile(string path, byte[] data, CancellationToken ct)
    {
        await File.WriteAllBytesAsync(path, data, ct);
    }
    
    public static async Task AppendBytesToFile(string path, byte[] data, CancellationToken ct)
    {
        await using var stream = new FileStream(path, FileMode.Append);
        await stream.WriteAsync(data, 0, data.Length, ct);
    }
}