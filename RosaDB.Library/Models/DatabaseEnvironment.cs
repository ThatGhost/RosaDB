using System.Collections.Generic;

namespace RosaDB.Library.Models;

public class DatabaseEnvironment
{
    public int Version { get; set; } = 0;
    public List<Cell> Cells { get; set; } = new();
}
