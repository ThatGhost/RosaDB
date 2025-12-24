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

        var newClientButton = new Button("Create new Client")
        {
            X = Pos.Center(),
            Y = 17
        };

        sendButton.Clicked += async () =>
        {
            try
            {
                if(ClientManager.Client is null) ClientManager.Client = new Client.Client("127.0.0.1", 7575);
                var result = await ClientManager.Client.SendQueryAsync(queryTextView.Text?.ToString() ?? "");
                resultTextView.Text = result;
            }
            catch
            {
                resultTextView.Text = "Could not connect to server at port 7575";
            }
        };

        newClientButton.Clicked += () =>
        {
            ClientManager.Client = new Client.Client("127.0.0.1", 7575);
            resultTextView.Text = "Created new client";
        };

        Add(queryLabel, queryTextView, resultLabel, resultTextView, sendButton, newClientButton);
    }
}