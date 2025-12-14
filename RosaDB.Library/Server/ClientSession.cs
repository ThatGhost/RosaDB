using System.Net.Sockets;
using System.Text;
using LightInject;
using RosaDB.Library.Core;
using RosaDB.Library.MoqQueries;

namespace RosaDB.Library.Server;

public class ClientSession(TcpClient client, Scope scope)
{
    public async Task HandleClient()
    {
        var stream = client.GetStream();
        while (true)
        {
            var buffer = new byte[1024];
            var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
            if (bytesRead == 0)
            {
                break;
            }

            var query = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            await scope.GetInstance<UseDatabaseQuery>().Execute("testy");
            
            // var result = await executor.Execute(this, query, CancellationToken.None);
            Result result = Result.Success();
            
            var response = result.IsSuccess ? "Success" : result.Error?.Message ?? "An unknown error occurred.";
            var responseBuffer = Encoding.UTF8.GetBytes(response);
            await stream.WriteAsync(responseBuffer, 0, responseBuffer.Length);
        }
    }
}
