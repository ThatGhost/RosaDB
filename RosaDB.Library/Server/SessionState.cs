using RosaDB.Library.Models;

namespace RosaDB.Library.Server;

public sealed class SessionState
{
    public Database? CurrentDatabase { get; set; }
}
