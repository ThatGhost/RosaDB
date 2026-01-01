using System.Text;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Serializers;

public static class LogSerializer
{
    // TODO add versioning to log structure
    public static byte[] Serialize(Log log)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(0); 
        
        writer.Write(log.Id);
        writer.Write(log.IsDeleted);
        writer.Write(log.Date.ToBinary());
        writer.Write(log.TupleData.Length);
        writer.Write(log.TupleData);

        var length = (int)ms.Position - 4;
        ms.Position = 0;
        writer.Write(length);

        return ms.ToArray();
    }

    public static Log? Deserialize(Stream stream)
    {
        if (stream.Position >= stream.Length) return null;

        var lengthBytes = new byte[4];
        int read = stream.Read(lengthBytes, 0, 4);
        if (read < 4) return null;

        int length = BitConverter.ToInt32(lengthBytes);
        
        if (stream.Length - stream.Position < length) return null;
        
        using var reader = new BinaryReader(stream, Encoding.UTF8, true); // true to leave stream open
        
        long startPos = stream.Position;

        var log = new Log()
        {
            Id = reader.ReadInt64(),
            IsDeleted = reader.ReadBoolean(),
            Date = DateTime.FromBinary(reader.ReadInt64()),
        };
        int tupleLength = reader.ReadInt32();
        log.TupleData = reader.ReadBytes(tupleLength);

        long bytesRead = stream.Position - startPos;
        if (bytesRead != length) stream.Position = startPos + length;
            
        return log;
    }

    public static async Task<Log?> DeserializeAsync(Stream stream, CancellationToken ct = default)
    {
         var lengthBytes = new byte[4];
         int read = await stream.ReadAsync(lengthBytes, 0, 4, ct);
         if (read < 4) return null;
         
         int length = BitConverter.ToInt32(lengthBytes);

         var buffer = new byte[length];
         int totalRead = 0;
         while (totalRead < length)
         {
             int r = await stream.ReadAsync(buffer, totalRead, length - totalRead, ct);
             if (r == 0) return null; // Unexpected EOF
             totalRead += r;
         }

         using var ms = new MemoryStream(buffer);
         using var reader = new BinaryReader(ms);

         var log = new Log()
         {
             Id = reader.ReadInt64(),
             IsDeleted = reader.ReadBoolean(),
             Date = DateTime.FromBinary(reader.ReadInt64()),
         };
         int tupleLength = reader.ReadInt32();
         log.TupleData = reader.ReadBytes(tupleLength);
         
         return log;
    }
}
