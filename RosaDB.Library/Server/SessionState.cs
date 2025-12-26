using RosaDB.Library.Models;

namespace RosaDB.Library.Server;

public class SessionState
{
    public virtual Database? CurrentDatabase { get; set; }
}
