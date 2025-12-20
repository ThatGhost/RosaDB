namespace RosaDB.Library.StorageEngine.Serializers;

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
    
    public static async Task<byte[]> ReadBytesFromFile(string path, long offset, int count, CancellationToken ct)
    {
        if (!File.Exists(path)) return [];

        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
        if (offset >= stream.Length) return [];

        stream.Seek(offset, SeekOrigin.Begin);

        if (count == -1)
        {
            // Read to the end of the file
            int bytesToRead = (int)(stream.Length - offset);
            var buffer = new byte[bytesToRead];
            await stream.ReadExactlyAsync(buffer, 0, bytesToRead, ct);
            return buffer;
        }
        else
        {
            var buffer = new byte[count];
            await stream.ReadExactlyAsync(buffer, 0, count, ct);
            return buffer;
        }
    }
}