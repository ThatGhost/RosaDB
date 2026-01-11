using System.Text;
using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Serializers;

public static class RowSerializer
{
    public static Result<byte[]> Serialize(Row row)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        var columns = row.Columns;
        var values = row.Values;

        writer.Write(columns.Length);

        // 1. Write Null Bitmap
        int bitmapLen = (columns.Length + 7) / 8;
        byte[] nullBitmap = new byte[bitmapLen];
        for (int i = 0; i < columns.Length; i++)
        {
            if (values[i] == null)
            {
                nullBitmap[i / 8] |= (byte)(1 << (i % 8));
            }
        }
        writer.Write(nullBitmap);

        // 2. Write Values
        for (int i = 0; i < columns.Length; i++)
        {
            var value = values[i];
            
            // Skip nulls as they are already marked in the bitmap
            if (value == null) continue;

            var type = columns[i].DataType;
            switch (type)
            {
                case DataType.INT:
                    writer.Write((int)value);
                    break;
                case DataType.BIGINT:
                    writer.Write((long)value);
                    break;
                case DataType.VARCHAR:
                {
                    var str = (string)value;
                    var bytes = Encoding.UTF8.GetBytes(str);
                    writer.Write(bytes.Length);
                    writer.Write(bytes);
                    break;
                }
                case DataType.TEXT:
                {
                    var str = (string)value;
                    var bytes = Encoding.UTF8.GetBytes(str);
                    writer.Write(bytes.Length);
                    writer.Write(bytes);
                    break;
                }
                case DataType.BOOLEAN:
                    writer.Write((bool)value);
                    break;
                default:
                    return new Error(ErrorPrefixes.DataError, $"Data type {type} is not supported for serialization.");
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

            var persistedColumnCount = reader.ReadInt32();
            
            // 1. Read Null Bitmap
            int bitmapLen = (persistedColumnCount + 7) / 8;
            byte[] nullBitmap = reader.ReadBytes(bitmapLen);
            
            if (nullBitmap.Length != bitmapLen)
                return new Error(ErrorPrefixes.DataError, "Serialized data is too short for the expected schema (missing null bitmap).");

            for (int i = 0; i < columns.Length; i++)
            {
                if (i >= persistedColumnCount)
                {
                    values[i] = null;
                    continue;
                }

                // Check if the value is null based on the bitmap
                bool isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
                
                if (isNull)
                {
                    values[i] = null;
                    continue;
                }

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
                    {
                        var length = reader.ReadInt32();
                        var bytes = reader.ReadBytes(length);
                        values[i] = Encoding.UTF8.GetString(bytes);
                        break;
                    }
                    case DataType.TEXT:
                    {
                        var length = reader.ReadInt32();
                        var bytes = reader.ReadBytes(length);
                        values[i] = Encoding.UTF8.GetString(bytes);
                        break;
                    }
                    case DataType.BOOLEAN:
                        values[i] = reader.ReadBoolean();
                        break;
                    default:
                        return new Error(ErrorPrefixes.DataError, $"Unknown or unsupported data type: {type}");
                }
            }
            
            if(values.Length != columns.Length)
                return new Error(ErrorPrefixes.DataError, "Column count mismatch after deserialization.");
            
            return Row.Create(values, columns);
        }
        catch (EndOfStreamException)
        {
            return new Error(ErrorPrefixes.DataError, "Data is in an old, incompatible format. Please migrate data to use schema evolution features.");
        }
        catch (Exception) { return new CriticalError(); }
    }
}