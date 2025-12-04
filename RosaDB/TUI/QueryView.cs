using System.Threading;
using RosaDB.Library.QueryEngine;
using Terminal.Gui;

namespace RosaDB.TUI
{
    public class QueryView : View
    {
        private readonly QueryExecutor _queryExecutor;
        private readonly TextView _queryInput;
        private readonly TextView _resultsView;
        
        public QueryView()
        {
            _queryExecutor = new QueryExecutor();
            
            Width = Dim.Fill();
            Height = Dim.Fill();

            var queryLabel = new Label("Enter your query:")
            {
                X = 1,
                Y = 1
            };

            _queryInput = new TextView()
            {
                X = 1,
                Y = 2,
                Width = Dim.Fill(1),
                Height = 5
            };

            var runButton = new Button("Run Query")
            {
                X = Pos.Center(),
                Y = Pos.Bottom(_queryInput) + 1
            };
            
            var resultsLabel = new Label("Result:")
            {
                X = 1,
                Y = Pos.Bottom(runButton) + 1
            };

            _resultsView = new TextView()
            {
                X = 1,
                Y = Pos.Bottom(resultsLabel),
                Width = Dim.Fill(1),
                Height = Dim.Fill(1),
                ReadOnly = true
            };
            
            runButton.Clicked += async () => await OnRunQueryClicked();

            Add(queryLabel, _queryInput, runButton, resultsLabel, _resultsView);
        }

        private async Task OnRunQueryClicked()
        {
            var query = _queryInput.Text.ToString();
            if (string.IsNullOrWhiteSpace(query))
            {
                return;
            }

            var result = await _queryExecutor.Execute(query, CancellationToken.None);

            if (result.IsSuccess)
            {
                _resultsView.Text = "Query executed successfully.";
            }
            else
            {
                _resultsView.Text = $"Error: {result.Error.Message}";
            }
        }
    }
}
