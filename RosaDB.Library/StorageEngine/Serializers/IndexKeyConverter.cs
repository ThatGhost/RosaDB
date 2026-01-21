using System;
using System.Globalization;
using System.Text;

namespace RosaDB.Library.StorageEngine.Serializers
{
    public static class IndexKeyConverter
    {
        public static byte[] ToByteArray(object? value)
        {
            return value switch
            {
                null => [], // Represents a null key
                byte[] b => b,
                string s => Encoding.UTF8.GetBytes(s),
                byte bVal => [bVal],
                short sVal => BitConverter.GetBytes((int)sVal),
                int iVal => BitConverter.GetBytes(iVal),
                long lVal => BitConverter.GetBytes(lVal),
                float fVal => BitConverter.GetBytes(fVal),
                double dVal => BitConverter.GetBytes(dVal),
                bool bVal => BitConverter.GetBytes(bVal),
                DateTime dt => BitConverter.GetBytes(dt.ToBinary()),
                decimal dec => Encoding.UTF8.GetBytes(dec.ToString(CultureInfo.InvariantCulture)),
                _ => Encoding.UTF8.GetBytes(value.ToString() ?? string.Empty) // Fallback for other types
            };
        }

        public static object? FromByteArray(byte[] bytes, Models.DataType type)
        {
            if (bytes.Length == 0)
            {
                return null;
            }
            
            switch (type)
            {
                case Models.DataType.INT:
                    return BitConverter.ToInt32(bytes, 0);
                case Models.DataType.BIGINT:
                case Models.DataType.LONG:
                    return BitConverter.ToInt64(bytes, 0);
                case Models.DataType.VARCHAR:
                case Models.DataType.TEXT:
                case Models.DataType.CHAR:
                case Models.DataType.CHARACTER:
                    return Encoding.UTF8.GetString(bytes);
                case Models.DataType.BOOLEAN:
                    return BitConverter.ToBoolean(bytes, 0);
                case Models.DataType.FLOAT:
                    return BitConverter.ToSingle(bytes, 0);
                case Models.DataType.SMALLINT:
                    return (short)BitConverter.ToInt32(bytes, 0); // Matching ToByteArray logic
                case Models.DataType.DECIMAL:
                case Models.DataType.NUMBER:
                    return decimal.Parse(Encoding.UTF8.GetString(bytes), CultureInfo.InvariantCulture);
                default:
                    throw new NotSupportedException($"Unsupported data type for byte conversion: {type}");
            }
        }
    }
}
