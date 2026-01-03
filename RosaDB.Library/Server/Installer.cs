using LightInject;
using RosaDB.Library.MoqQueries;
using RosaDB.Library.Query;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.Validation;
using System.Net.Sockets;
using RosaDB.Library.Query.Queries;
using System.IO.Abstractions;
using RosaDB.Library.StorageEngine.Interfaces;

namespace RosaDB.Library.Server;

public static class Installer
{
    public static void Install(ServiceContainer container)
    {
        container.Register<TcpClient, Scope, ClientSession>((_, client, scope) => new ClientSession(client, scope));
        
        container.RegisterScoped<SessionState>();
        container.RegisterScoped<ILogManager, LogManager>();
        container.RegisterScoped<IIndexManager, IndexManager>();
        container.RegisterScoped<ICellManager, CellManager>();
        
        container.RegisterTransient<IFileSystem, FileSystem>();
        container.RegisterTransient<IFolderManager, FolderManager>();
        container.RegisterTransient<LogCondenser>();
        container.RegisterTransient<RootManager>();
        container.RegisterTransient<IDatabaseManager, DatabaseManager>();
        container.RegisterTransient<DataValidator>();
        container.RegisterTransient<StringToDataParser>();
        container.RegisterTransient<QueryTokenizer>();
        container.RegisterTransient<QueryPlanner>();
    }
}