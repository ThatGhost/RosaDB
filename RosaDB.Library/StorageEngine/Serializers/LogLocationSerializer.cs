using CSharpTest.Net.Serialization;
using System.IO;

namespace RosaDB.Library.StorageEngine.Serializers;

public class LogLocationSerializer : ISerializer<LogLocation>
{
    public LogLocation ReadFrom(Stream stream)
    {
        using var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, true);
        var segmentNumber = reader.ReadInt32();
        var offset = reader.ReadInt64();
        return new LogLocation(segmentNumber, offset);
    }

    public void WriteTo(LogLocation value, Stream stream)
    {
        using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, true);
        writer.Write(value.SegmentNumber);
        writer.Write(value.Offset);
    }
}