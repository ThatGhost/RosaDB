using System.Text;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.StorageEngine.Serializers;

public static class IndexSerializer
{
    public static byte[] Serialize(IndexHeader header)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(header.Version);
        writer.Write(header.CellName);
        writer.Write(header.TableName);
        writer.Write(header.InstanceHash);
        writer.Write(header.SegmentNumber);

        return ms.ToArray();
    }

    public static IndexHeader? DeserializeHeader(Stream stream)
    {
        try 
        {
            using var reader = new BinaryReader(stream, Encoding.UTF8, true);
            
            var version = reader.ReadInt32();
            var cellName = reader.ReadString();
            var tableName = reader.ReadString();
            var instanceHash = reader.ReadString();
            var segmentNumber = reader.ReadInt32();
            
            return new IndexHeader(cellName, tableName, instanceHash, segmentNumber, version);
        }
        catch
        {
            return null;
        }
    }

    public static byte[] Serialize(SparseIndexEntry entry)
    {
        // 20 bytes fixed size (4 int + 8 long + 8 long)
        var bytes = new byte[20];
        BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), entry.Version);
        BitConverter.TryWriteBytes(bytes.AsSpan(4, 8), entry.Key);
        BitConverter.TryWriteBytes(bytes.AsSpan(12, 8), entry.Offset);
        return bytes;
    }

    public static SparseIndexEntry? DeserializeEntry(Stream stream)
    {
        int length = 20;
        if (stream.Position + length > stream.Length) return null;

        var buffer = new byte[length];
        int read = stream.Read(buffer, 0, length);
        if (read < length) return null;

        int version = BitConverter.ToInt32(buffer, 0);
        long key = BitConverter.ToInt64(buffer, 4);
        long offset = BitConverter.ToInt64(buffer, 12);
        
        return new SparseIndexEntry(key, offset, version);
    }
}
