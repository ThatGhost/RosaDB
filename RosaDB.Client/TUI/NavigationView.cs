using System.Linq;
using Terminal.Gui;

namespace RosaDB.Client.TUI;

public sealed class NavigationView : View
{
    public NavigationView()
    {
        Width = 20;
        Height = Dim.Fill();

        var menu = new ListView(new[] { "Home", "Query", "Logs", "Websocket", "Seed Data" })
        {
            X = 0,
            Y = 0,
            Width = Dim.Fill(),
            Height = Dim.Fill()
        };

        menu.SelectedItemChanged += (args) =>
        {
            var contentView = SuperView.Subviews.FirstOrDefault(v => v is ContentView) as ContentView;
            contentView?.ShowView(args.Item);
        };

        Add(menu);
    }
}