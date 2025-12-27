using System.Collections.Generic;
using System.Data;
using System.Linq;
using Terminal.Gui;

namespace RosaDB.Client.TUI;

public class QueryView : View
{
    private readonly Label _statusLabel;
    private readonly TableView _tableView;

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

        _statusLabel = new Label("")
        {
            X = 1,
            Y = 9,
            Width = Dim.Fill() - 2,
            Height = 2
        };

        _tableView = new TableView()
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
            Y = Pos.Bottom(_tableView) + 1
        };

        var newClientButton = new Button("Create new Client")
        {
            X = Pos.Center(),
            Y = Pos.Bottom(sendButton) + 1
        };
        
        newClientButton.Clicked += () =>
        {
            try
            {
                ClientManager.Client = new Client.Client("127.0.0.1", 7575);
                _statusLabel.Text = "Client re-initialized successfully.";
                _tableView.Table = null; // Clear previous results
                _tableView.Visible = false;
            }
            catch
            {
                _statusLabel.Text = "Could not connect to server at port 7575. Check if server is running.";
            }
        };
        
        sendButton.Clicked += async () =>
        {
            try
            {
                if(ClientManager.Client is null) ClientManager.Client = new Client.Client("127.0.0.1", 7575);
                var response = await ClientManager.Client.SendQueryAsync(queryTextView.Text?.ToString() ?? "");

                _tableView.Table = null;
                _tableView.Visible = false;

                if (response is null)
                {
                    _statusLabel.Text = "Error: Did not receive a response from the server.";
                    return;
                }

                _statusLabel.Text = $"{response.Message}\n({response.DurationMs:F2} ms, {response.RowsAffected} rows affected)";

                if (response.Rows != null && response.Rows.Count > 0)
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

                    _tableView.Table = dataTable;
                    _tableView.Visible = true;
                }
            }
            catch
            {
                _statusLabel.Text = "Could not connect to server at port 7575";
            }
        };

        Add(queryLabel, queryTextView, resultLabel, _statusLabel, _tableView, sendButton, newClientButton);
    }
}