using System.Collections.Generic;
using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.Validation
{
    public class StringToDataParser
    {
        public Result<List<object>> ParseData(string[] data, DataType[] dataType)
        {
            var parsedData = new List<object>();
            if (data.Length != dataType.Length) 
                return new Error(ErrorPrefixes.QueryParsingError, "The number of data items does not match the number of data types.");

            for (int i = 0; i < data.Length; i++)
            {
                var value = data[i];
                var type = dataType[i];
                var result = Parse(value, type);
                if (result.IsFailure) return result.Error;
                parsedData.Add(result.Value);
            }
            return parsedData;
        }

        public static Result<object> Parse(string value, DataType type)
        {
            switch (type)
            {
                case DataType.INT:
                    if (int.TryParse(value, out int intValue)) return intValue;
                    return new DataTypeError(DataType.INT, value);
                case DataType.VARCHAR:
                    return value;
                case DataType.CHAR:
                    if (char.TryParse(value, out char charValue)) return charValue;
                    return new DataTypeError(DataType.CHAR, value);
                case DataType.CHARACTER:
                    if (char.TryParse(value, out char characterValue)) return characterValue;
                    return new DataTypeError(DataType.CHARACTER, value);
                case DataType.BIGINT:
                    if (long.TryParse(value, out long longValue)) return longValue;
                    return new DataTypeError(DataType.BIGINT, value);
                case DataType.BOOLEAN:
                    if (bool.TryParse(value, out bool boolValue)) return boolValue;
                    return new DataTypeError(DataType.BOOLEAN, value);
                case DataType.NUMBER:
                    if (decimal.TryParse(value, out decimal decimalValue)) return decimalValue; // needs to be updated to have precision and scale
                    return new DataTypeError(DataType.NUMBER, value);
                case DataType.LONG:
                    if (long.TryParse(value, out long long2Value)) return long2Value;
                    return new DataTypeError(DataType.LONG, value);
                case DataType.FLOAT:
                    if (float.TryParse(value, out float floatValue)) return floatValue;
                    return new DataTypeError(DataType.FLOAT, value);
                case DataType.SMALLINT:
                    if (short.TryParse(value, out short shortValue)) return shortValue;
                    return new DataTypeError(DataType.SMALLINT, value);
                default:
                    return new Error(ErrorPrefixes.DataError, $"Unsupported data type: {type}");
            }
        }

        private record DataTypeError(DataType dataType, object value) : Error(ErrorPrefixes.DataError, $"Invalid {dataType.ToString()} value: {value.ToString()}");
    }
}