using LightInject;
using RosaDB.Library.MoqQueries;
using RosaDB.Library.Query;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.Validation;
using System.Net.Sockets;
using RosaDB.Library.Query.Queries;

namespace RosaDB.Library.Server;

public static class Installer
{
    public static void Install(ServiceContainer container)
    {
        container.Register<TcpClient, Scope, ClientSession>((_, client, scope) => new ClientSession(client, scope));
        container.RegisterScoped<SessionState>();
        container.RegisterScoped<LogManager>();
        
        container.RegisterTransient<LogCondenser>();
        container.RegisterTransient<RootManager>();
        container.RegisterTransient<DatabaseManager>();
        container.RegisterScoped<CellManager>();

        container.Register<DataValidator>();
        container.Register<StringToDataParser>();
        container.Register<QueryTokenizer>();
        container.Register<QueryPlanner>();

        InstallMoqQueries(container);
    }

    private static void InstallMoqQueries(ServiceContainer container)
    {
        container.Register<CreateCellQuery>();
        container.Register<WriteLogAndCommitQuery>();
        container.Register<CreateDatabaseQuery>();
        container.Register<UseDatabaseQuery>();
        container.Register<InitializeDbQuery>();
        container.Register<CreateTableDefinition>();
        container.Register<GetAllLogsQuery>();
        container.Register<GetCellLogsQuery>();
        container.Register<UpdateCellLogsQuery>();
        container.Register<RandomDeleteLogsQuery>();
    }
}