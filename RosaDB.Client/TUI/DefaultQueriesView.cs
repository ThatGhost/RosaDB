using RosaDB.Client.TUI.Persistence;
using Terminal.Gui;

namespace RosaDB.Client.TUI
{
    public sealed class DefaultQueriesView : View
    {
        private readonly ListView _listView;
        private readonly SavedQueriesManager _savedQueriesManager;
        private List<string> _queries = [];

        public Action<string>? OnQuerySelected;

        public DefaultQueriesView()
        {
            Width = Dim.Fill();
            Height = Dim.Fill();

            _savedQueriesManager = new SavedQueriesManager();

            var savedQueriesLabel = new Label("Saved Queries:")
            {
                X = 0,
                Y = 0,
            };

            _listView = new ListView()
            {
                X = 0,
                Y = 1,
                Width = Dim.Fill(),
                Height = Dim.Fill() - 2,
            };
            
            _listView.OpenSelectedItem += (args) =>
            {
                if (_queries.Count > 0)
                {
                    OnQuerySelected?.Invoke(_queries[args.Item]);
                }
            };
            
            var deleteButton = new Button("Delete Selected")
            {
                X = Pos.Center(),
                Y = Pos.Bottom(_listView) + 1
            };
            
            deleteButton.Clicked += () =>
            {
                if (_listView.SelectedItem >= 0 && _listView.SelectedItem < _queries.Count)
                {
                    var queryToDelete = _queries[_listView.SelectedItem];
                    _savedQueriesManager.DeleteQuery(queryToDelete);
                }
            };
            
            Add(savedQueriesLabel, _listView, deleteButton);

            SavedQueriesManager.QueriesChanged += LoadQueries;
            LoadQueries();
        }

        private void LoadQueries()
        {
            _queries = _savedQueriesManager.GetQueries();
            _listView.SetSource(_queries);
        }
    }
}