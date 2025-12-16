using System.Net.Sockets;
using System.Text;
using LightInject;
using RosaDB.Library.Core;
using RosaDB.Library.MoqQueries;
using RosaDB.Library.Query;

namespace RosaDB.Library.Server;

public class ClientSession(TcpClient client, Scope scope)
{
    public async Task HandleClient()
    {
        await scope.GetInstance<UseDatabaseQuery>().Execute("db");
        var queryTokenizer = scope.GetInstance<QueryTokenizer>();
        var stream = client.GetStream();
        while (true)
        {
            var buffer = new byte[1024];
            var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
            if (bytesRead == 0)
            {
                break;
            }

            DateTime init = DateTime.Now;
            var query = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            //await scope.GetInstance<InitializeDbQuery>().Execute();
            //await scope.GetInstance<CreateDatabaseQuery>().Execute("db");
            //await scope.GetInstance<UseDatabaseQuery>().Execute("db");
            //await scope.GetInstance<CreateCellQuery>().Execute("cell");
            //await scope.GetInstance<CreateTableDefinition>().Execute("cell", "table");
            //await scope.GetInstance<WriteLogAndCommitQuery>().Execute("cell", "table", query);
            //await scope.GetInstance<GetCellLogsQuery>().Execute("cell","table", [4]);
            //await scope.GetInstance<UpdateCellLogsQuery>().Execute("cell","table", [4], query);
            //await scope.GetInstance<GetAllLogsQuery>().Execute("cell","table");

            Result result = Result.Success();
            var tokens = queryTokenizer.TokenizeQuery(query);
            if (tokens.IsFailure) result = Result.Failure(tokens.Error!);

            DateTime end = DateTime.Now;
            TimeSpan duration = end - init;

            var response = result.IsSuccess ? $"{DateTime.Now.ToShortTimeString()} : Success in {duration.Milliseconds}ms" : result.Error?.Message ?? "An unknown error occurred.";
            var responseBuffer = Encoding.UTF8.GetBytes(response);
            await stream.WriteAsync(responseBuffer, 0, responseBuffer.Length);
        }
    }
}
