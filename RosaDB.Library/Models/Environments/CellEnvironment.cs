using RosaDB.Library.Core;

namespace RosaDB.Library.Models.Environments;

public class CellEnvironment
{
    public int Version { get; set; } = Constants.Version;
    public Column[] Columns { get; set; } = [];
    public Table[] Tables { get; set; } = [];
}