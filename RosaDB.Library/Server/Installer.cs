using LightInject;
using RosaDB.Library.Query;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.Validation;
using System.Net.Sockets;
using System.IO.Abstractions;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.Websockets;

namespace RosaDB.Library.Server;

public static class Installer
{
    public static void Install(ServiceContainer container)
    {
        container.RegisterSingleton<ISubscriptionManager, SubscriptionManager>();
        container.RegisterSingleton<SocketManager>();

        container.RegisterScoped<SessionState>();
        container.RegisterScoped<ILogManager, LogManager>();
        container.RegisterScoped<IIndexManager, IndexManager>();
        container.RegisterScoped<ICellManager, CellManager>();
        container.RegisterScoped<WebsocketQueryPlanner>();
        
        container.Register<TcpClient, Scope, ClientSession>((_, client, scope) => new ClientSession(client, scope));
        container.RegisterTransient<IFileSystem, FileSystem>();
        container.RegisterTransient<IFolderManager, FolderManager>();
        container.RegisterTransient<LogCondenser>();
        container.RegisterTransient<RootManager>();
        container.RegisterTransient<IDatabaseManager, DatabaseManager>();
        container.RegisterTransient<DataValidator>();
        container.RegisterTransient<TokensToDataParser>();
        container.RegisterTransient<QueryTokenizer>();
        container.RegisterTransient<QueryPlanner>();
    }
}