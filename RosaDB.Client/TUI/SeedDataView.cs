using System.Globalization;
using System.Threading;
using Terminal.Gui;
using System.Collections.Generic;
using System;
using System.Threading.Tasks;

namespace RosaDB.Client.TUI
{
    public class SeedDataView : View
    {
        private readonly TextView _logOutput;
        private readonly Button _seedButton;
        private readonly List<string> _logLines = new();
        private readonly List<string> _errorMessages = new();
        private const int MaxLogLines = 1000;
        private CancellationTokenSource? _animationCts;

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
            try
            {
                ClientManager.Client ??= new Client.Client("127.0.0.1", 7575);
                {
                    var stream = ClientManager.Client.SendQueryAndStreamAsync(query);
                    await foreach (var response in stream)
                    {
                        if (response.Message.StartsWith("Error"))
                        {
                            _errorMessages.Add($"Query Failed: \"{query}\"\nError: {response.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _errorMessages.Add($"Connection Failed for query \"{query}\"\nError: {ex.Message}");
            }
        }

        private async Task SeedData()
        {
            _seedButton.Enabled = false;
            _logLines.Clear();
            _errorMessages.Clear();
            
            _animationCts = new CancellationTokenSource();
            var animationTask = AnimateDots(_animationCts.Token);

            try
            {
                await SendQuery("INITIALIZE;");
                await SendQuery("CREATE DATABASE seededDatabase;");
                await SendQuery("USE seededDatabase;");
                await SendQuery("CREATE CELL users (id INT INDEX, name TEXT, age INT);");
                await SendQuery("CREATE TABLE users.addresses (id INT PRIMARY KEY, street TEXT, city TEXT, zip_code TEXT);");
                await SendQuery("CREATE TABLE users.transactions (id INT PRIMARY KEY, amount TEXT, date TEXT);");
                await SendQuery("CREATE CELL warehouses (id INT INDEX, name TEXT, location TEXT);");
                await SendQuery("CREATE TABLE warehouses.inventory (id INT PRIMARY KEY, product_name TEXT, quantity INT, aisle INT);");
                await SendQuery("CREATE TABLE warehouses.shipments (id INT PRIMARY KEY, user_id INT, transaction_id INT, status TEXT);");
                
                await SendQuery("CREATE CELL products (id INT INDEX);");
                await SendQuery("CREATE TABLE products.details (id INT PRIMARY KEY, description TEXT);");
                await SendQuery("CREATE TABLE products.reviews (id INT PRIMARY KEY, user_id INT, rating INT, comment TEXT);");
                await SendQuery("CREATE TABLE products.daily_stats (day TEXT PRIMARY KEY, page_views INT, units_sold INT);");
                await SendQuery("CREATE TABLE products.daily_referrals (id INT PRIMARY KEY, day TEXT, source_url TEXT, view_count INT);");
                await SendQuery("CREATE TABLE products.daily_sales_by_region (id INT PRIMARY KEY, day TEXT, region_name TEXT, units_sold INT);");

                await SendQuery("CREATE CELL catalog (id INT INDEX, name TEXT);");
                await SendQuery("CREATE TABLE catalog.products (id INT PRIMARY KEY, name TEXT, category TEXT, price TEXT);");

                await SeedUserCellInstances();
                await SeedUserTables();
                await SeedWarehouseCellInstances();
                await SeedWarehouseTables();
                await SeedCatalog();
                await SeedProductCellInstances();
                await SeedProductTables();
                await SeedProductDailyStats();
            }
            catch (Exception ex)
            {
                _errorMessages.Add($"A critical error occurred during seeding: {ex.Message}");
            }
            finally
            {
                _animationCts.Cancel();
                try
                {
                    await animationTask;
                }
                catch (TaskCanceledException) { /* Expected */ }

                Application.MainLoop.Invoke(() =>
                {
                    if (_errorMessages.Count == 0)
                    {
                        _logOutput.Text = "Seeding process finished successfully.";
                    }
                    else
                    {
                        _logOutput.Text = "Seeding process finished with errors:\n\n" + string.Join("\n", _errorMessages);
                    }
                    _seedButton.Enabled = true;
                });
            }
        }

        private async Task AnimateDots(CancellationToken cancellationToken)
        {
            var dots = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                dots = (dots + 1) % 4; // 0, 1, 2, 3
                var dotString = new string('.', dots);
                Application.MainLoop.Invoke(() =>
                {
                    _logOutput.Text = $"Seeding database{dotString}";
                });
                await Task.Delay(300, cancellationToken);
            }
        }

        private async Task SeedProductDailyStats()
        {
            Log("Seeding product daily stats...");
            await SendQuery("BEGIN TRANSACTION;");
            var random = new Random();
            try
            {
                string[] referralSources = { "google.com", "facebook.com", "direct", "twitter.com" };
                string[] regions = { "North America", "Europe", "Asia", "South America" };

                for (int productId = 1; productId <= 4; productId++)
                {
                    for (int dayOffset = 0; dayOffset < 3; dayOffset++)
                    {
                        string day = $"2026-01-{15 + dayOffset}";
                        int pageViews = random.Next(100, 1000);
                        int unitsSold = random.Next(5, 50);
                        await SendQuery($"INSERT INTO products.daily_stats USING id = {productId} (day, page_views, units_sold) VALUES ('{day}', {pageViews}, {unitsSold});");
                        for (int i = 0; i < random.Next(1, 4); i++)
                        {
                            string source = referralSources[random.Next(referralSources.Length)];
                            int viewCount = random.Next(10, pageViews / 2);
                            await SendQuery($"INSERT INTO products.daily_referrals USING id = {productId} (id, day, source_url, view_count) VALUES ({i + 1}, '{day}', '{source}', {viewCount});");
                        }
                        for (int i = 0; i < random.Next(1, 3); i++)
                        {
                            string region = regions[random.Next(regions.Length)];
                            int regionUnits = random.Next(1, unitsSold / 2);
                            await SendQuery($"INSERT INTO products.daily_sales_by_region USING id = {productId} (id, day, region_name, units_sold) VALUES ({i + 1}, '{day}', '{region}', {regionUnits});");
                        }
                    }
                }
                await SendQuery("COMMIT;");
            }
            catch (Exception ex)
            {
                await SendQuery("ROLLBACK;");
                _errorMessages.Add($"Error during product daily stats seeding: {ex.Message}. Transaction was rolled back.");
            }
        }

        private async Task SeedCatalog()
        {
            Log("Seeding catalog...");
            await SendQuery("BEGIN TRANSACTION;");
            try
            {
                await SendQuery("INSERT CELL catalog (id, name) VALUES (1, 'main_catalog');");
                await SendQuery("INSERT INTO catalog.products USING id = 1 (id, name, category, price) VALUES (1, 'Laptop Pro X', 'Electronics', '1200.00');");
                await SendQuery("INSERT INTO catalog.products USING id = 1 (id, name, category, price) VALUES (2, 'Mechanical Keyboard', 'Electronics', '80.00');");
                await SendQuery("INSERT INTO catalog.products USING id = 1 (id, name, category, price) VALUES (3, 'Wireless Mouse', 'Electronics', '35.00');");
                await SendQuery("INSERT INTO catalog.products USING id = 1 (id, name, category, price) VALUES (4, 'Ergonomic Chair', 'Office Furniture', '300.00');");
                await SendQuery("COMMIT;");
            }
            catch (Exception ex)
            {
                await SendQuery("ROLLBACK;");
                _errorMessages.Add($"Error during catalog seeding: {ex.Message}. Transaction was rolled back.");
            }
        }

        private async Task SeedProductTables()
        {
            Log("Seeding product tables...");
            await SendQuery("BEGIN TRANSACTION;");
            var random = new Random();
            try
            {
                // Product 1: Laptop Pro X
                await SendQuery("INSERT INTO products.details USING id = 1 (id, description) VALUES (1, 'High performance laptop with 16GB RAM');");
                for (int i = 0; i < 100; i++)
                {
                    int userId = random.Next(1, 6);
                    int rating = random.Next(1, 6);
                    await SendQuery($"INSERT INTO products.reviews USING id = 1 (id, user_id, rating, comment) VALUES ({i + 1}, {userId}, {rating}, 'This is a random review {i + 1}');");
                }
                // Product 2: Mechanical Keyboard
                await SendQuery("INSERT INTO products.details USING id = 2 (id, description) VALUES (1, 'Clicky keys, great for typing');");
                for (int i = 0; i < 100; i++)
                {
                    int userId = random.Next(1, 6);
                    int rating = random.Next(1, 6);
                    await SendQuery($"INSERT INTO products.reviews USING id = 2 (id, user_id, rating, comment) VALUES ({i + 1}, {userId}, {rating}, 'This is a random review {i + 1}');");
                }
                // Product 3: Wireless Mouse
                await SendQuery("INSERT INTO products.details USING id = 3 (id, description) VALUES (1, 'Comfortable and responsive');");
                for (int i = 0; i < 100; i++)
                {
                    int userId = random.Next(1, 6);
                    int rating = random.Next(1, 6);
                    await SendQuery($"INSERT INTO products.reviews USING id = 3 (id, user_id, rating, comment) VALUES ({i + 1}, {userId}, {rating}, 'This is a random review {i + 1}');");
                }
                // Product 4: Ergonomic Chair
                await SendQuery("INSERT INTO products.details USING id = 4 (id, description) VALUES (1, 'Adjustable support for long hours');");
                for (int i = 0; i < 100; i++)
                {
                    int userId = random.Next(1, 6);
                    int rating = random.Next(1, 6);
                    await SendQuery($"INSERT INTO products.reviews USING id = 4 (id, user_id, rating, comment) VALUES ({i + 1}, {userId}, {rating}, 'This is a random review {i + 1}');");
                }
                await SendQuery("COMMIT;");
            }
            catch (Exception ex)
            {
                await SendQuery("ROLLBACK;");
                _errorMessages.Add($"Error during product table seeding: {ex.Message}. Transaction was rolled back.");
            }
        }

        private async Task SeedProductCellInstances()
        {
            Log("Creating product instances...");
            await SendQuery("INSERT CELL products (id) VALUES (1);");
            await SendQuery("INSERT CELL products (id) VALUES (2);");
            await SendQuery("INSERT CELL products (id) VALUES (3);");
            await SendQuery("INSERT CELL products (id) VALUES (4);");
        }

        private async Task SeedWarehouseTables()
        {
            Log("Seeding warehouse tables...");
            await SendQuery("BEGIN TRANSACTION;");
            try
            {
                await SendQuery("INSERT INTO warehouses.inventory USING id = 1 (id, product_name, quantity, aisle) VALUES (1, 'Laptop', 100, 1);");
                await SendQuery("INSERT INTO warehouses.inventory USING id = 1 (id, product_name, quantity, aisle) VALUES (2, 'Monitor', 150, 1);");
                await SendQuery("INSERT INTO warehouses.shipments USING id = 1 (id, user_id, transaction_id, status) VALUES (1, 1, 1, 'Shipped');");
                await SendQuery("INSERT INTO warehouses.shipments USING id = 1 (id, user_id, transaction_id, status) VALUES (2, 2, 1, 'Pending');");
                await SendQuery("INSERT INTO warehouses.inventory USING id = 2 (id, product_name, quantity, aisle) VALUES (1, 'Keyboard', 200, 2);");
                await SendQuery("INSERT INTO warehouses.inventory USING id = 2 (id, product_name, quantity, aisle) VALUES (2, 'Mouse', 250, 2);");
                await SendQuery("INSERT INTO warehouses.shipments USING id = 2 (id, user_id, transaction_id, status) VALUES (1, 3, 1, 'Delivered');");
                await SendQuery("INSERT INTO warehouses.inventory USING id = 3 (id, product_name, quantity, aisle) VALUES (1, 'Webcam', 75, 3);");
                await SendQuery("INSERT INTO warehouses.shipments USING id = 3 (id, user_id, transaction_id, status) VALUES (1, 4, 1, 'Shipped');");
                await SendQuery("COMMIT;");
            }
            catch (Exception ex)
            {
                await SendQuery("ROLLBACK;");
                _errorMessages.Add($"Error during warehouse table seeding: {ex.Message}. Transaction was rolled back.");
            }
        }

        private async Task SeedWarehouseCellInstances()
        {
            Log("Creating warehouse instances...");
            await SendQuery("INSERT CELL warehouses (id, name, location) VALUES (1, 'Main Warehouse', 'New York');");
            await SendQuery("INSERT CELL warehouses (id, name, location) VALUES (2, 'West Coast Hub', 'Los Angeles');");
            await SendQuery("INSERT CELL warehouses (id, name, location) VALUES (3, 'East Coast Depot', 'Boston');");
        }

        private async Task SeedUserTables()
        {
            Log("Seeding user tables...");
            await SendQuery("BEGIN TRANSACTION;");
            try
            {
                // User 1: Alice Smith
                await SendQuery("INSERT INTO users.addresses USING id = 1 (id, street, city, zip_code) VALUES (1, '123 Maple Street', 'New York', '10001');");
                await SendQuery("INSERT INTO users.transactions USING id = 1 (id, amount, date) VALUES (1, '150.75', '2026-01-15');");
                await SendQuery("INSERT INTO users.transactions USING id = 1 (id, amount, date) VALUES (2, '25.50', '2026-01-16');");
                // User 2: Bob Johnson
                await SendQuery("INSERT INTO users.addresses USING id = 2 (id, street, city, zip_code) VALUES (1, '456 Oak Avenue', 'London', 'SW1A 0AA');");
                await SendQuery("INSERT INTO users.transactions USING id = 2 (id, amount, date) VALUES (1, '99.99', '2026-01-12');");
                await SendQuery("INSERT INTO users.transactions USING id = 2 (id, amount, date) VALUES (2, '42.00', '2026-01-14');");
                // User 3: Charlie Brown
                await SendQuery("INSERT INTO users.addresses USING id = 3 (id, street, city, zip_code) VALUES (1, '789 Pine Lane', 'Tokyo', '100-0001');");
                await SendQuery("INSERT INTO users.transactions USING id = 3 (id, amount, date) VALUES (1, '1200.00', '2026-01-10');");
                await SendQuery("INSERT INTO users.transactions USING id = 3 (id, amount, date) VALUES (2, '500.25', '2026-01-11');");
                // User 4: David Williams
                await SendQuery("INSERT INTO users.addresses USING id = 4 (id, street, city, zip_code) VALUES (1, '101 Elm Drive', 'Paris', '75001');");
                await SendQuery("INSERT INTO users.transactions USING id = 4 (id, amount, date) VALUES (1, '75.00', '2026-01-18');");
                await SendQuery("INSERT INTO users.transactions USING id = 4 (id, amount, date) VALUES (2, '125.00', '2026-01-19');");
                // User 5: Eve Jones
                await SendQuery("INSERT INTO users.addresses USING id = 5 (id, street, city, zip_code) VALUES (1, '212 Birch Circle', 'Sydney', '2000');");
                await SendQuery("INSERT INTO users.transactions USING id = 5 (id, amount, date) VALUES (1, '300.00', '2026-01-13');");
                await SendQuery("INSERT INTO users.transactions USING id = 5 (id, amount, date) VALUES (2, '45.99', '2026-01-17');");
                await SendQuery("COMMIT;");
            }
            catch (Exception ex)
            {
                await SendQuery("ROLLBACK;");
                _errorMessages.Add($"Error during user table seeding: {ex.Message}. Transaction was rolled back.");
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

        private void Log(string message, string? time = null)
        {
            string logEntry = $"{DateTime.Now:HH:mm:ss} {(time is null ? "" : $"- took: {time}ms")} - {message}";
            _logLines.Insert(0, logEntry);
            if (_logLines.Count > MaxLogLines)
            {
                _logLines.RemoveAt(_logLines.Count - 1);
            }
        }
    }
}