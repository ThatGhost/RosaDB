using Terminal.Gui;
using System;
using System.Threading.Tasks;

namespace RosaDB.Client.TUI
{
    public class SeedDataView : View
    {
        private readonly TextView _logOutput;
        private readonly Button _seedButton;

        public SeedDataView()
        {
            Width = Dim.Fill();
            Height = Dim.Fill();

            _seedButton = new Button("Seed Data")
            {
                X = Pos.Center(),
                Y = 1
            };
            _seedButton.Clicked += async () => await SeedData();

            _logOutput = new TextView()
            {
                X = 0,
                Y = 3,
                Width = Dim.Fill(),
                Height = Dim.Fill(),
                ReadOnly = true
            };

            Add(_seedButton, _logOutput);
        }

        private async Task SeedData()
        {
            _seedButton.Enabled = false;
            Log("Starting data seeding...");

            try
            {
                ClientManager.Client ??= new Client.Client("127.0.0.1", 7575);

                await CreateAndSeedCells();
            }
            catch (Exception ex)
            {
                Log($"Error: {ex.Message}");
            }
            finally
            {
                Log("Seeding process finished.");
                _seedButton.Enabled = true;
            }
        }

        private async Task CreateAndSeedCells()
        {
            string[] cells = { "customers", "products", "orders", "employees" };
            string[,] tables = {
                { "profiles", "addresses" },
                { "details", "inventory" },
                { "summaries", "items" },
                { "records", "departments" }
            };

            for (int i = 0; i < cells.Length; i++)
            {
                var cell = cells[i];
                await SendQuery($"CREATE CELL {cell} (id INT PRIMARY KEY, name TEXT);");

                for (int j = 0; j < 2; j++)
                {
                    var table = tables[i, j];
                    await SendQuery($"CREATE TABLE {cell}.{table} (id INT PRIMARY KEY, data TEXT);");
                    await SeedTable(cell, table);
                }
            }
        }

        private async Task SeedTable(string cell, string table)
        {
            Log($"Seeding table {cell}.{table}...");
            for (int i = 0; i < 100; i++)
            {
                await SendQuery($"INSERT INTO {cell}.{table} USING id = {i} (id, data) VALUES ({i}, 'some random data {i}');");
            }
        }

        private async Task SendQuery(string query)
        {
            Log($"Sending: {query}");
            try
            {
                var stream = ClientManager.Client.SendQueryAndStreamAsync(query);
                await foreach (var response in stream)
                {
                    Log($"Response: {response.Message} ({response.RowsAffected} rows affected)");
                }
            }
            catch (Exception ex)
            {
                Log($"Query failed: {ex.Message}");
            }
        }

        private void Log(string message)
        {
            Application.MainLoop.Invoke(() =>
            {
                _logOutput.Text += $"{DateTime.Now:HH:mm:ss} - {message}\n";
            });
        }
    }
}
