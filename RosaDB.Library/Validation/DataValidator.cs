using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.Validation
{
    public class DataValidator(CellManager cellManager)
    {
        public async Task<Result> ValidateDataForCellAndTable(string cellName, string tableName, string[] collumnNames, object[] data)
        {
            if (collumnNames.Length != data.Length) return new Error(ErrorPrefixes.DataError, "Number of data does not match amount columns");
            
            var cellEnvironment = await cellManager.GetEnvironment(cellName);
            if (cellEnvironment.IsFailure) return cellEnvironment.Error;

            Table? tableFromEnvironment = cellEnvironment.Value.Tables.FirstOrDefault(t => t.Name == tableName);
            if (tableFromEnvironment == null) return new Error(ErrorPrefixes.DataError, $"Table {tableName} does not exist in cell {cellName}");

            for (int i = 0; i < collumnNames.Length; i++)
            {
                Column? column = tableFromEnvironment.Columns.FirstOrDefault(c => c.Name == collumnNames[i]);
                if (column == null) return new Error(ErrorPrefixes.DataError, $"Column {collumnNames[i]} does not exist in table {tableName}");

                if (!IsDataTypeValid(data[i], column.DataType)) return new Error(ErrorPrefixes.DatatypeError, $"Data does not match Datatype {column.DataType}");
            }

            return Result.Success();
        }

        private bool IsDataTypeValid(object data, DataType dataType)
        {
            return dataType switch
            {
                DataType.INT => data is int,
                DataType.VARCHAR => data is string,
                DataType.BIGINT => data is long,
                DataType.BOOLEAN => data is bool,
                _ => false
            };
        }
    }
}
