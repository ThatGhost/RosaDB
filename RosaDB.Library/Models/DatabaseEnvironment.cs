using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

public class DatabaseEnvironment
{
    public int Version { get; set; } = Constants.Version;
    public List<Cell> Cells { get; set; } = new();
}
