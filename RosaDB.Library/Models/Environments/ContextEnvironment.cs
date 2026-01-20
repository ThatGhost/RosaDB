using RosaDB.Library.Core;
using System;
using System.Linq;
using System.Collections.Generic;
using RosaDB.Library.Models;

namespace RosaDB.Library.Models.Environments;

public class ContextEnvironment
{
    public int Version { get; set; } = Constants.Version;
    public Column[] Columns { get; set; } = [];
    public Table[] Tables { get; set; } = [];
    
    public Column[] IndexColumns => Columns.Where(c => c.IsIndex).ToArray();

    public object?[] GetIndexValues(Row instanceRow)
    {
        var indexValues = new List<object?>();
        foreach (var indexCol in IndexColumns)
        {
            var colIndex = Array.FindIndex(instanceRow.Columns, c => c.Name.Equals(indexCol.Name, StringComparison.OrdinalIgnoreCase));
            if (colIndex != -1)
            {
                indexValues.Add(instanceRow.Values[colIndex]);
            }
        }
        return indexValues.ToArray();
    }
}
