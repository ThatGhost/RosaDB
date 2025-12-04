using RosaDB.Server;

var server = new Server("127.0.0.1", 7485);
await server.Start();