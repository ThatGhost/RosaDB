using System.Data;
using Terminal.Gui;

namespace RosaDB.Client.TUI;

public class QueryView : View
{
    public QueryView()
    {
        Width = Dim.Fill();
        Height = Dim.Fill();

        var leftPane = new View()
        {
            Width = Dim.Percent(70),
            Height = Dim.Fill()
        };

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

        var statusLabel = new Label("")
        {
            X = 1,
            Y = 9,
            Width = Dim.Fill() - 2,
            Height = 2
        };

        var tableView = new TableView()
        {
            X = 1,
            Y = 11,
            Width = Dim.Fill() - 2,
            Height = Dim.Fill() - 12,
            Visible = false
        };

        var sendButton = new Button("Send")
        {
            X = Pos.Center(),
            Y = Pos.Bottom(tableView) + 1
        };

        var newClientButton = new Button("Create new Client")
        {
            X = Pos.Center(),
            Y = Pos.Bottom(sendButton) + 1
        };

        leftPane.Add(queryLabel, queryTextView, resultLabel, statusLabel, tableView, sendButton, newClientButton);

        var separator = new LineView(Terminal.Gui.Graphs.Orientation.Vertical)
        {
            X = Pos.Right(leftPane),
            Height = Dim.Fill()
        };

        var defaultQueriesView = new DefaultQueriesView()
        {
            X = Pos.Right(separator),
            Width = Dim.Fill(),
            Height = Dim.Fill()
        };

        defaultQueriesView.OnQuerySelected += (query) =>
        {
            queryTextView.Text = query;
        };
        
        newClientButton.Clicked += () =>
        {
            try
            {
                ClientManager.Client = new Client.Client("127.0.0.1", 7575);
                statusLabel.Text = "Client re-initialized successfully.";
                tableView.Table = null;
                tableView.Visible = false;
            }
            catch
            {
                statusLabel.Text = "Could not connect to server at port 7575. Check if server is running.";
            }
        };
        
        sendButton.Clicked += async () =>
        {
            try
            {
                ClientManager.Client ??= new Client.Client("127.0.0.1", 7575);
                var response = await ClientManager.Client.SendQueryAsync(queryTextView.Text?.ToString() ?? "");

                tableView.Table = null;
                tableView.Visible = false;

                if (response is null)
                {
                    statusLabel.Text = "Error: Did not receive a response from the server.";
                    return;
                }

                statusLabel.Text = $"{response.Message}\n({response.DurationMs:F2} ms, {response.RowsAffected} rows affected)";

                if (response.Rows is { Count: > 0 })
                {
                    var dataTable = new DataTable();
                    var firstRow = response.Rows.First();
                    foreach (var colName in firstRow.Keys)
                    {
                        dataTable.Columns.Add(colName, typeof(string));
                    }

                    foreach (var rowDict in response.Rows)
                    {
                        var newRow = dataTable.NewRow();
                        foreach (var col in rowDict)
                        {
                            newRow[col.Key] = col.Value?.ToString() ?? "NULL";
                        }
                        dataTable.Rows.Add(newRow);
                    }

                    tableView.Table = dataTable;
                    tableView.Visible = true;
                }
            }
            catch
            {
                statusLabel.Text = "Could not connect to server at port 7575";
            }
        };

        Add(leftPane, separator, defaultQueriesView);
    }
}