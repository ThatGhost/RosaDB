using System.Collections.Generic;
using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.Validation
{
    public class DataParser
    {
        public Result<List<object>> ParseData(string[] data, DataType[] dataType)
        {
            var parsedData = new List<object>();
            if (data.Length != dataType.Length) return new Error(ErrorPrefixes.DataError, "The number of data items does not match the number of data types.");

            for (int i = 0; i < data.Length; i++)
            {
                var value = data[i];
                var type = dataType[i];

                switch (type)
                {
                    case DataType.INT:
                        if (int.TryParse(value, out int intValue)) parsedData.Add(intValue);
                        else return new Error(ErrorPrefixes.DataError, "Invalid INT value: " + value);

                        break;
                    case DataType.VARCHAR:
                        parsedData.Add(value);

                        break;
                    case DataType.BIGINT:
                        if (long.TryParse(value, out long longValue)) parsedData.Add(longValue);
                        else return new Error(ErrorPrefixes.DataError, $"Invalid BIGINT value: {value}");

                        break;
                    case DataType.BOOLEAN:
                        if (bool.TryParse(value, out bool boolValue)) parsedData.Add(boolValue);
                        else return new Error(ErrorPrefixes.DataError, $"Invalid BOOLEAN value: {value}");

                        break;
                    default:
                        return new Error(ErrorPrefixes.DataError, $"Unsupported data type: {type}");
                }
            }
            return parsedData;
        }
    }
}