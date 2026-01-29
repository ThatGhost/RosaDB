using System.Text.Json.Serialization;
using RosaDB.Library.Core;

namespace RosaDB.Library.Models;

[method: JsonConstructor]
public class Database(string name, List<Module> modules)
{
    public string Name { get; } = name;
    public List<Module> Modules { get; } = modules;

    public static Result<Database> Create(string name)
    {
        if(string.IsNullOrWhiteSpace(name)) return new Error(ErrorPrefixes.DataError, "Database name cannot be empty.");
        return new Database(name, []);
    }
    
    public Module? GetModule(string name) => Modules.FirstOrDefault(m => m.Name == name);
}