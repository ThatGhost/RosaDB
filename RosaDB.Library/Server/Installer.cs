using LightInject;
using RosaDB.Library.Query;
using RosaDB.Library.StorageEngine;
using System.Net.Sockets;
using System.IO.Abstractions;
using RosaDB.Library.Query.TokenParsers;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.Websockets;
using RosaDB.Library.Websockets.Interfaces;
using RosaDB.Library.Server.Logging;
using RosaDB.Library.Server.Interfaces;

namespace RosaDB.Library.Server;

public static class Installer
{
    public static void Install(ServiceContainer container)
    {
        // Logging Services
        container.RegisterSingleton<ILogQueue, LogQueue>();
        container.RegisterSingleton<LogBackgroundService>();
        container.RegisterInstance<IServiceContainer>(container);
        container.RegisterSingleton<ISubscriptionManager, SubscriptionManager>();
        container.RegisterSingleton<SocketManager>();

        container.RegisterScoped<ISystemLogPublisher, SystemLogPublisher>();
        container.RegisterScoped<SessionState>();
        container.RegisterScoped<ILogReader, LogReader>();
        container.RegisterScoped<ILogWriter, LogWriter>();
        container.RegisterSingleton<WriteAheadLogCache>();
        container.RegisterScoped<IIndexManager, IndexManager>();
        container.RegisterScoped<IContextManager, ContextManager>();
        container.RegisterScoped<WebsocketQueryPlanner>();
        
        container.Register<TcpClient, Scope, ClientSession>((factory, client, scope) => new ClientSession(client, scope, scope.GetInstance<ISystemLogPublisher>()));
        container.RegisterTransient<IFileSystem, FileSystem>();
        container.RegisterTransient<IFolderManager, FolderManager>();
        container.RegisterTransient<ILogCondenser, LogCondenser>();
        container.RegisterTransient<RootManager>();
        container.RegisterTransient<IDatabaseManager, DatabaseManager>();
        container.RegisterTransient<DataValidator>();
        container.RegisterTransient<TokensToDataParser>();
        container.RegisterTransient<QueryTokenizer>();
        container.RegisterTransient<QueryPlanner>();
    }
}