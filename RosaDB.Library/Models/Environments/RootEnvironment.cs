using RosaDB.Library.Core;

namespace RosaDB.Library.Models.Environments
{
    public class RootEnvironment
    {
        public int Version { get; set; } = Constants.Version;
        public List<string> DatabaseNames { get; set; } = [];
    }
}
