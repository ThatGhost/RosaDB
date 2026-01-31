using LightInject;
using RosaDB.Library.Query;
using RosaDB.Library.StorageEngine;
using System.Net.Sockets;
using System.IO.Abstractions;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.Websockets;
using RosaDB.Library.Websockets.Interfaces;
using RosaDB.Library.Server.Interfaces;

namespace RosaDB.Library.Server;

public static class Installer
{
    public static void Install(ServiceContainer container)
    {
        // Logging Services
        container.RegisterInstance<IServiceContainer>(container);
        container.RegisterSingleton<ISubscriptionManager, SubscriptionManager>();
        container.RegisterSingleton<SocketManager>();

        container.RegisterScoped<SessionState>();
        container.RegisterScoped<ILogReader, LogReader>();
        container.RegisterScoped<ILogWriter, LogWriter>();
        container.RegisterSingleton<WriteAheadLogCache>();
        container.RegisterScoped<IIndexManager, IndexManager>();
        container.RegisterScoped<IModuleManager, ModuleManager>();
        container.RegisterScoped<WebsocketQueryPlanner>();
        
        container.Register<TcpClient, Scope, ClientSession>((factory, client, scope) => new ClientSession(client, scope, scope.GetInstance<ISystemLogPublisher>()));
        container.RegisterTransient<IFileSystem, FileSystem>();
        container.RegisterTransient<IFolderManager, FolderManager>();
        container.RegisterTransient<IRowManager, RowManager>();
        container.RegisterTransient<ILogCondenser, LogCondenser>();
        container.RegisterTransient<IDatabaseManager, DatabaseManager>();
        container.RegisterTransient<DataValidator>();
        container.RegisterTransient<TokensToDataParser>();
        container.RegisterTransient<QueryTokenizer>();
        container.RegisterTransient<QueryPlanner>();
    }
}