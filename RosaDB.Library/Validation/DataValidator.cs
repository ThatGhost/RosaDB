using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.Validation
{
    public class DataValidator()
    {
        public static Result Validate(object? value, Column column)
        {
            if (value == null && !column.IsNullable) return new Error(ErrorPrefixes.DataError, $"Column '{column.Name}' cannot be null.");
            if (value != null)
            {
                // TODO Validate data type 
                // Eg. char length for VARCHAR, range for INT, etc.
            }
            return Result.Success();
        }
    }
}
