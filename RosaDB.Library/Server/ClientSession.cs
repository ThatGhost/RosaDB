using System.Net.Sockets;
using System.Text;
using LightInject;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.MoqQueries;
using RosaDB.Library.Query;

namespace RosaDB.Library.Server;

public class ClientSession(TcpClient client, Scope scope)
{
    public async Task HandleClient()
    {
        var queryTokenizer = scope.GetInstance<QueryTokenizer>();
        var queryPlanner = scope.GetInstance<QueryPlanner>();
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
            await scope.GetInstance<InitializeDbQuery>().Execute();
            await scope.GetInstance<CreateDatabaseQuery>().Execute("db");
            await scope.GetInstance<UseDatabaseQuery>().Execute("db");
            await scope.GetInstance<CreateCellQuery>().Execute("cell");
            await scope.GetInstance<CreateTableDefinition>().Execute("cell", "table");
            await scope.GetInstance<WriteLogAndCommitQuery>().Execute("cell", "table", query);
            //await scope.GetInstance<RandomDeleteLogsQuery>().Execute("cell", "table", [1]);
            await scope.GetInstance<UpdateCellLogsQuery>().Execute("cell", "table", [2], "Updated: " + query);
            await scope.GetInstance<GetCellLogsQuery>().Execute("cell", "table", [2]);
            //await scope.GetInstance<GetAllLogsQuery>().Execute("cell", "table");

            QueryResult result = new QueryResult("succes");

            //var tokens = queryTokenizer.TokenizeQuery(query);
            //if (tokens.IsFailure) result = tokens.Error;
            //else
            //{
            //    var queryPlan = queryPlanner.CreateQueryPlanFromTokens(tokens.Value);
            //    if (queryPlan.IsFailure) result = queryPlan.Error;
            //    else
            //    {
            //        result = await queryPlan.Value.Execute();
            //    }
            //}

            DateTime end = DateTime.Now;
            TimeSpan duration = end - init;

            var response = $"{DateTime.Now.ToShortTimeString()} - {duration.TotalMilliseconds} ms : {result.Message}";
            if (result.RowsAffected > 0) response += $"\n -- {result.RowsAffected} Rows affected";

            var responseBuffer = Encoding.UTF8.GetBytes(response);
            await stream.WriteAsync(responseBuffer, 0, responseBuffer.Length);
        }
    }
}
