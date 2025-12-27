using System;
using System.Text;

namespace RosaDB.Library.StorageEngine.Serializers
{
    public static class IndexKeyConverter
    {
        public static byte[] ToByteArray(object? value)
        {
            return value switch
            {
                null => Array.Empty<byte>(), // Represents a null key
                byte[] b => b,
                string s => Encoding.UTF8.GetBytes(s),
                object o when o is byte bVal => [bVal],
                object o when o is short sVal => BitConverter.GetBytes((int)sVal),
                object o when o is int iVal => BitConverter.GetBytes(iVal),
                object o when o is long lVal => BitConverter.GetBytes(lVal),
                object o when o is float fVal => BitConverter.GetBytes(fVal),
                object o when o is double dVal => BitConverter.GetBytes(dVal),
                object o when o is bool bVal => BitConverter.GetBytes(bVal),
                object o when o is DateTime dt => BitConverter.GetBytes(dt.ToBinary()),
                decimal dec => Encoding.UTF8.GetBytes(dec.ToString()),
                _ => Encoding.UTF8.GetBytes(value.ToString() ?? string.Empty) // Fallback for other types
            };
        }
    }
}
