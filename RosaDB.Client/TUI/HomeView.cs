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

        var startServerButton = new Button("Stop Server")
        {
            X = Pos.Center(),
            Y = Pos.Bottom(artLabel) + 2
        };

        Add(artLabel, startServerButton);
    }
}
