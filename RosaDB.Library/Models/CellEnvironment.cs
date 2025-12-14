using System.Collections.Generic;

namespace RosaDB.Library.Models;

public class CellEnvironment
{
    public int Version { get; set; } = 0;
    public List<Table> Tables { get; set; } = new();
}
