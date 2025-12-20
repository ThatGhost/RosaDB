using System.Text;
using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine;

public static class RowSerializer
{
    public static byte[] Serialize(Row row)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        var columns = row.Columns;
        var values = row.Values;

        for (int i = 0; i < columns.Length; i++)
        {
            var type = columns[i].DataType;
            var value = values[i];
            
            switch (type)
            {
                case DataType.INT:
                    writer.Write((int)value!);
                    break;
                case DataType.BIGINT:
                    writer.Write((long)value!);
                    break;
                case DataType.VARCHAR:
                    var str = (string)value!;
                    var bytes = Encoding.UTF8.GetBytes(str);
                    writer.Write(bytes.Length);
                    writer.Write(bytes);
                    break;
                case DataType.BOOLEAN:
                    writer.Write((bool)value!);
                    break;
                default:
                    throw new NotSupportedException($"Data type {type} is not supported for serialization.");
            }
        }

        return ms.ToArray();
    }

    public static Result<Row> Deserialize(byte[] data, Column[] columns)
    {
        try
        {
            using var ms = new MemoryStream(data);
            using var reader = new BinaryReader(ms);

            var values = new object?[columns.Length];

            for (int i = 0; i < columns.Length; i++)
            {
                var type = columns[i].DataType;

                switch (type)
                {
                    case DataType.INT:
                        values[i] = reader.ReadInt32();
                        break;
                    case DataType.BIGINT:
                        values[i] = reader.ReadInt64();
                        break;
                    case DataType.VARCHAR:
                        var length = reader.ReadInt32();
                        var bytes = reader.ReadBytes(length);
                        values[i] = Encoding.UTF8.GetString(bytes);
                        break;
                    case DataType.BOOLEAN:
                        values[i] = reader.ReadBoolean();
                        break;
                    default:
                        return new Error(ErrorPrefixes.DataError, "Unknown data type");
                }
            }
            
            if(values.Length != columns.Length)
                return new Error(ErrorPrefixes.DataError, "Column count mismatch");
            
            return new Row(values, columns);
        }
        catch { return new CriticalError(); }
    }
}
