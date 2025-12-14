using System.Collections.Generic;

namespace RosaDB.Library.Models
{
    public class Environment
    {
        public int Version { get; set; } = 0;
        public List<string> DatabaseNames { get; set; } = [];
    }
}
