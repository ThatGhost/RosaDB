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
    }
}
