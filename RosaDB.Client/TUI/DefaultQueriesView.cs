using System;
using System.Collections.Generic;
using Terminal.Gui;

namespace RosaDB.Client.TUI
{
    public sealed class DefaultQueriesView : View
    {
        private readonly ListView _listView;

        public Action<string>? OnQuerySelected;

        public DefaultQueriesView()
        {
            Width = Dim.Fill();
            Height = Dim.Fill();

            List<(string Name, string Query)> queries = [
                ("Show Cells", "SHOW CELLS;"),
                ("Show Tables in Group", "SHOW TABLES IN sales;"),
                // DDL
                ("Initialize RosaDB", "INITIALIZE;"),
                ("Create Database", "CREATE DATABASE my_db;"),
                ("Use Database", "USE my_db;"),
                ("Create Cell Group", "CREATE CELL sales (name TEXT INDEX, region TEXT, is_active BOOLEAN);"),
                ("Create Table for Group", "CREATE TABLE sales.transactions (id INT PRIMARY KEY, product TEXT, amount INT);"),
                // Cell Instance Management
                ("Insert Cell Instance", "INSERT CELL sales (name, region) VALUES ('q4', 'EMEA');"),
                ("Update Cell Instance", "UPDATE CELL sales USING name = 'q4' SET is_active = FALSE;"),
                ("Delete Cell Instance", "DELETE CELL sales USING name = 'q4';"),
                // DML
                ("Select All from Group", "SELECT * FROM sales.transactions;"), // Cross-cell query
                ("Select with Data Filter", "SELECT * FROM sales.transactions WHERE amount > 100;"),
                ("Select with Cell Filter", "SELECT * FROM sales.transactions USING name = 'q4';"),
                ("Select with Both Filters", "SELECT * FROM sales.transactions USING name = 'q4' WHERE amount > 100;"),
                ("Insert Data", "INSERT INTO sales.transactions USING name = 'q4' (id, product, amount) VALUES (1, 'item', 50);"),
            ];

            _listView = new ListView(queries.ConvertAll(q => q.Name))
            {
                X = 0,
                Y = 0,
                Width = Dim.Fill(),
                Height = Dim.Fill(),
            };

            _listView.OpenSelectedItem += (args) =>
            {
                OnQuerySelected?.Invoke(queries[args.Item].Query);
            };

            Add(_listView);
        }
    }
}
