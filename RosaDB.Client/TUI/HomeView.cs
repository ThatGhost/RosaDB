using Terminal.Gui;

namespace RosaDB.Client.TUI;

public class HomeView : View
{
    public HomeView()
    {
        Width = Dim.Fill();
        Height = Dim.Fill();

        var rosaArt = @"
██████╗  ██████╗ ███████╗ █████╗      ██████╗  ██████╗ 
██╔══██╗██╔═══██╗██╔════╝██╔══██╗     ██╔══██╗ ██╔══██╗
██████╔╝██║   ██║███████╗███████║     ██║  ██║ ██████╔╝
██╔══██╗██║   ██║╚════██║██╔══██║     ██║  ██║ ██╔══██╗
██║  ██║╚██████╔╝███████║██║  ██║     ██████╔╝ ██████╔╝
╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝     ╚═════╝  ╚═════╝ 
";
        var artLabel = new Label(rosaArt)
        {
            X = Pos.Center(),
            Y = Pos.Center(),
        };

        var startServerButton = new Button("Start Server")
        {
            X = Pos.Center(),
            Y = Pos.Bottom(artLabel) + 2
        };

        startServerButton.Clicked += () =>
        {
            if (ServerManager.Server == null)
            {
                ServerManager.Server = new RosaDB.Server.Server("127.0.0.1", 7485);
                Task.Run(() => ServerManager.Server.Start());
                startServerButton.Text = "Stop Server";
            }
            else
            {
                ServerManager.Server.Stop();
                ServerManager.Server = null;
                startServerButton.Text = "Start Server";
            }
        };

        Add(artLabel, startServerButton);
    }
}
