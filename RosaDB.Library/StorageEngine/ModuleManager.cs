using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.StorageEngine
{
    public class ModuleManager(SessionState sessionState, ILogWriter logWriter, ILogReader logReader, IIndexManager indexManager) : IModuleManager
    {
        public Result CreateModuleInstance(string module, string instanceHash, Row instanceData)
        {
            
        }

        public async Task<Result<Row>> GetModuleInstance(string module, string instanceHash)
        {
            
        }

        public Task<Result> DeleteModuleInstance(string module, string instanceHash)
        {
            throw new NotImplementedException();
        }

        public async Task<Result<IEnumerable<Row>>> GetAllModuleInstances(string module)
        {
            
        }
    }
}