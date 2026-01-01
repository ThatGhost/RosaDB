using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine.Serializers;

public static class ByteReaderWriter
{
    public static async Task<byte[]> ReadBytesFromFile(IFileSystem fileSystem, string path, CancellationToken ct)
    {
        return !fileSystem.File.Exists(path) ? [] : await fileSystem.File.ReadAllBytesAsync(path, ct);
    }

    public static async Task WriteBytesToFile(IFileSystem fileSystem, string path, byte[] data, CancellationToken ct)
    {
        await fileSystem.File.WriteAllBytesAsync(path, data, ct);
    }
}