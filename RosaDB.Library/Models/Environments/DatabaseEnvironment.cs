using RosaDB.Library.Core;

namespace RosaDB.Library.Models.Environments;

public class DatabaseEnvironment
{
    public int Version { get; set; } = Constants.Version;
    public List<Context> Contexts { get; set; } = [];
}
