using System.Net.Sockets;
using LightInject;
using RosaDB.Library.MoqQueries;
using RosaDB.Library.StorageEngine;

namespace RosaDB.Library.Server;

public static class Installer
{
    public static void Install(ServiceContainer container)
    {
        container.Register<TcpClient, Scope, ClientSession>((_, client, scope) => new ClientSession(client, scope));
        container.RegisterScoped<SessionState>();
        container.RegisterSingleton<LogManager>();
        container.RegisterTransient<LogCondenser>();
        container.RegisterSingleton<RootManager>();
        InstallMoqQueries(container);
    }

    private static void InstallMoqQueries(ServiceContainer container)
    {
        container.Register<CreateCellQuery>();
        container.Register<WriteLogAndCommitQuery>();
        container.Register<CreateDatabaseQuery>();
        container.Register<UseDatabaseQuery>();
    }
}