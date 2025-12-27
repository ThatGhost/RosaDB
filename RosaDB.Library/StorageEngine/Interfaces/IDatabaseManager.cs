using RosaDB.Library.Core;
using RosaDB.Library.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RosaDB.Library.StorageEngine.Interfaces
{
    public interface IDatabaseManager
    {
        Task<Result> CreateDatabaseEnvironment(Database database);
        Task<Result> CreateCell(string cellName, Column[] columns);
        Task<Result> DeleteCell(string cellName);
    }
}
