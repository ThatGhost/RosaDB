using Terminal.Gui;

namespace RosaDB.Client.TUI;

public class QueryView : View
{
    public QueryView()
    {
        Width = Dim.Fill();
        Height = Dim.Fill();

        var queryLabel = new Label("Query:")
        {
            X = 1,
            Y = 1
        };

        var queryTextView = new TextView()
        {
            X = 1,
            Y = 2,
            Width = Dim.Fill() - 2,
            Height = 5
        };

        var resultLabel = new Label("Result:")
        {
            X = 1,
            Y = 8
        };

        var resultTextView = new TextView()
        {
            X = 1,
            Y = 9,
            Width = Dim.Fill() - 2,
            Height = 5,
            ReadOnly = true
        };

        var sendButton = new Button("Send")
        {
            X = Pos.Center(),
            Y = 15
        };

        sendButton.Clicked += async () =>
        {
            if (ClientManager.Client == null)
            {
                ClientManager.Client = new Client.Client("127.0.0.1", 7485);
            }
            var result = await ClientManager.Client.SendQueryAsync(queryTextView.Text?.ToString() ?? "");
            resultTextView.Text = result;
        };

        Add(queryLabel, queryTextView, resultLabel, resultTextView, sendButton);
    }
}