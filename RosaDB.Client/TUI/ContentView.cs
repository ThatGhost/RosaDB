using Terminal.Gui;

namespace RosaDB.Client.TUI;

public class ContentView : View
{
    private readonly HomeView _homeView = new();
    private readonly QueryView _queryView = new();
    private readonly LogsView _logsView = new();
    private readonly WebsocketClientView _websocketClientView = new();
    private readonly SeedDataView _seedDataView = new();

    public ContentView()
    {
        X = 21;
        Width = Dim.Fill();
        Height = Dim.Fill();

        ShowView(0);
    }

    public void ShowView(int index)
    {
        RemoveAll();
        switch (index)
        {
            case 0:
                Add(_homeView);
                break;
            case 1:
                Add(_queryView);
                break;
            case 2:
                Add(_logsView);
                break;
            case 3:
                Add(_websocketClientView);
                break;
            case 4:
                Add(_seedDataView);
                break;
        }
    }
}