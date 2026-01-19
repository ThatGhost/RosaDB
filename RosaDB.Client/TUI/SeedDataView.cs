using Terminal.Gui;
using System;

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

        private async Task SendQuery(string query)
        {
            Log($"Sending: {query}");
            try
            {
                ClientManager.Client ??= new Client.Client("127.0.0.1", 7575);
                {
                    var stream = ClientManager.Client.SendQueryAndStreamAsync(query);
                    await foreach (var response in stream)
                    {
                        Log($"Response: {response.Message} ({response.RowsAffected} rows affected)");
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"Query failed: {ex.Message}");
            }
        }

        private async Task SeedData()
        {
            _seedButton.Enabled = false;
            Log("Starting data seeding...");

            try
            {
                await SendQuery("INITIALIZE;");
                await SendQuery("CREATE DATABASE seededDatabase;");
                await SendQuery("USE seededDatabase;");
                await SendQuery("CREATE CELL users (id INT INDEX, name TEXT, age INT);");
                await SendQuery("CREATE TABLE users.addresses (id INT PRIMARY KEY, street TEXT, city TEXT, zip_code TEXT);");
                await SendQuery("CREATE TABLE users.transactions (id INT PRIMARY KEY, amount REAL, date TEXT);");
                await SeedUserCellInstances();
                await SeedUserTables();
            }
            catch (Exception ex)
            {
                Log($"Error during seeding: {ex.Message}");
            }
            finally
            {
                Log("Seeding process finished.");
                _seedButton.Enabled = true;
            }
        }

        private async Task SeedUserTables()
        {
            Log("Seeding user tables...");
            await SendQuery("BEGIN TRANSACTION;");
            try
            {
                // User 1: Alice Smith
                await SendQuery("INSERT INTO users.addresses USING id = 1 (id, street, city, zip_code) VALUES (1, '123 Maple Street', 'New York', '10001');");
                await SendQuery("INSERT INTO users.transactions USING id = 1 (id, amount, date) VALUES (1, 150.75, '2026-01-15');");
                await SendQuery("INSERT INTO users.transactions USING id = 1 (id, amount, date) VALUES (2, 25.50, '2026-01-16');");

                // User 2: Bob Johnson
                await SendQuery("INSERT INTO users.addresses USING id = 2 (id, street, city, zip_code) VALUES (1, '456 Oak Avenue', 'London', 'SW1A 0AA');");
                await SendQuery("INSERT INTO users.transactions USING id = 2 (id, amount, date) VALUES (1, 99.99, '2026-01-12');");
                await SendQuery("INSERT INTO users.transactions USING id = 2 (id, amount, date) VALUES (2, 42.00, '2026-01-14');");

                // User 3: Charlie Brown
                await SendQuery("INSERT INTO users.addresses USING id = 3 (id, street, city, zip_code) VALUES (1, '789 Pine Lane', 'Tokyo', '100-0001');");
                await SendQuery("INSERT INTO users.transactions USING id = 3 (id, amount, date) VALUES (1, 1200.00, '2026-01-10');");
                await SendQuery("INSERT INTO users.transactions USING id = 3 (id, amount, date) VALUES (2, 500.25, '2026-01-11');");

                // User 4: David Williams
                await SendQuery("INSERT INTO users.addresses USING id = 4 (id, street, city, zip_code) VALUES (1, '101 Elm Drive', 'Paris', '75001');");
                await SendQuery("INSERT INTO users.transactions USING id = 4 (id, amount, date) VALUES (1, 75.00, '2026-01-18');");
                await SendQuery("INSERT INTO users.transactions USING id = 4 (id, amount, date) VALUES (2, 125.00, '2026-01-19');");

                // User 5: Eve Jones
                await SendQuery("INSERT INTO users.addresses USING id = 5 (id, street, city, zip_code) VALUES (1, '212 Birch Circle', 'Sydney', '2000');");
                await SendQuery("INSERT INTO users.transactions USING id = 5 (id, amount, date) VALUES (1, 300.00, '2026-01-13');");
                await SendQuery("INSERT INTO users.transactions USING id = 5 (id, amount, date) VALUES (2, 45.99, '2026-01-17');");

                await SendQuery("COMMIT;");
            }
            catch (Exception ex)
            {
                await SendQuery("ROLLBACK;");
                Log($"Error during table seeding: {ex.Message}. Transaction was rolled back.");
            }
        }

        private async Task SeedUserCellInstances()
        {
            Log("Creating user instances...");
            await SendQuery("INSERT CELL users (id, name, age) VALUES (1, 'Alice Smith', 30);");
            await SendQuery("INSERT CELL users (id, name, age) VALUES (2, 'Bob Johnson', 25);");
            await SendQuery("INSERT CELL users (id, name, age) VALUES (3, 'Charlie Brown', 35);");
            await SendQuery("INSERT CELL users (id, name, age) VALUES (4, 'David Williams', 40);");
            await SendQuery("INSERT CELL users (id, name, age) VALUES (5, 'Eve Jones', 28);");
        }

        private void Log(string message)
        {
            Application.MainLoop.Invoke(() =>
            {
                _logOutput.Text = $"{DateTime.Now:HH:mm:ss} - {message}\n" + _logOutput.Text;
            });
        }
    }
}
