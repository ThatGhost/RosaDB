using Terminal.Gui;

namespace RosaDB.Client.TUI;

public sealed class HomeView : View
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

        Add(artLabel);
    }
}
