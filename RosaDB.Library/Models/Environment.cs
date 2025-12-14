using RosaDB.Library.Core;

namespace RosaDB.Library.Models
{
    public class Environment
    {
        public int Version { get; set; } = Constants.Version;
        public List<string> DatabaseNames { get; set; } = [];
    }
}
