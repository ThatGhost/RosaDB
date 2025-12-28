using System;
using System.Collections.Generic;
using Terminal.Gui;

namespace RosaDB.Client.TUI
{
    public class DefaultQueriesView : View
    {
        private ListView _listView;
        private List<(string Name, string Query)> _queries;

        public Action<string>? OnQuerySelected;

        public DefaultQueriesView()
        {
            Width = Dim.Fill();
            Height = Dim.Fill();

            _queries = new List<(string, string)>
            {
                ("Select All", "SELECT * FROM sales.transactions;"),
                ("Select with Data Filter", "SELECT * FROM sales.transactions WHERE amount > 100;"),
                ("Select with Cell Filter", "SELECT * FROM sales.transactions USING name = 'q4';"),
                ("Select with Both Filters", "SELECT * FROM sales.transactions USING name = 'q4' WHERE amount > 100;"),
                ("Insert Data", "INSERT INTO sales.transactions USING name = 'q4' (id, product, amount) VALUES (1, 'item', 50);"),
                ("Create Database", "CREATE DATABASE my_db;"),
                ("Use Database", "USE DATABASE my_db;"),
                ("Create Cell Group", "CREATE CELL sales (name TEXT PRIMARY KEY, region TEXT, is_active BOOLEAN);"),
                ("Create Table for Group", "CREATE TABLE sales.transactions (id INT PRIMARY KEY, product TEXT, amount INT);")
            };

            _listView = new ListView(_queries.ConvertAll(q => q.Name))
            {
                X = 0,
                Y = 0,
                Width = Dim.Fill(),
                Height = Dim.Fill(),
            };

            _listView.OpenSelectedItem += (args) =>
            {
                OnQuerySelected?.Invoke(_queries[args.Item].Query);
            };

            Add(_listView);
        }
    }
}
