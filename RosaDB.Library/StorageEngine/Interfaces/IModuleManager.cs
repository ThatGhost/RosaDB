using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using System.Threading.Tasks;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface IModuleManager
    {
        Result CreateModuleInstance(string module, string instanceHash, Row instanceData);
        Task<Result<Row>> GetModuleInstance(string module, string instanceHash);
        Task<Result> DeleteModuleInstance(string module, string instanceHash);
        Task<Result<IEnumerable<Row>>> GetAllModuleInstances(string module);
    }
}
