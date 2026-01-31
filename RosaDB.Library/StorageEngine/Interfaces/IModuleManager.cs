using RosaDB.Library.Core;
using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface IModuleManager
    {
        public Task<Result> InsertModuleInstance(string module, Row instanceData);
        public Task<Result<Row>> GetModuleInstance(string module, string instanceHash);
        public Task<Result> DeleteModuleInstance(string module, string instanceHash);
        public IAsyncEnumerable<Row> GetAllModuleInstances(string module);
    }
}
