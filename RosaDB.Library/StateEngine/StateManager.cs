namespace RosaDB.Library.StateEngine;

public static class StateManager
{
    private static DatabaseState _state;
    private static string? _usedDatabaseName;

    public static async Task<DatabaseState> GetDatabaseState()
    {
        
        return _state;
    }

    public static string? GetUsedDatabaseName() => _usedDatabaseName;

    public static async Task UpdateUsedDatabase(string? usedDatabase)
    {
        _state = _state with { UsedDatabase = usedDatabase};
        _usedDatabaseName = usedDatabase;
    }
}