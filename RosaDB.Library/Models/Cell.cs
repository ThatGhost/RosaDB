using System;
using System.Collections.Generic;
using System.Linq;

namespace RosaDB.Library.Models;

public class Cell
{
    public string Name { get; }
    public List<Column> Columns { get; }

    public Cell(string name, List<Column> columns)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Cell name cannot be empty.", nameof(name));
        if (columns == null || !columns.Any())
            throw new ArgumentException("Cell must have at least one column.", nameof(columns));
        if (!columns.Any(c => c.IsPrimaryKey))
            throw new InvalidOperationException("Cell must have at least one primary key column.");

        Name = name;
        Columns = columns;
    }
}