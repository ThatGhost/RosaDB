using Terminal.Gui;
using static Terminal.Gui.View;
using Terminal.Gui.Graphs;

namespace RosaDB.Client.TUI;

public class MainWindow : Window
{
    public MainWindow()
    {
        Title = "RosaDB";

        var navigationView = new NavigationView();
        var contentView = new ContentView();
        var separator = new LineView(Orientation.Vertical)
        {
            X = 20,
            Height = Dim.Fill()
        };

        Add(navigationView, separator, contentView);
    }
}