using System;
using System.Collections.Generic;
using System.Linq;

namespace RosaDB.Library.Models;

public class Cell
{
    public string Name { get; }

    public Cell(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be empty.", nameof(name));
        
        Name = name;
    }
}