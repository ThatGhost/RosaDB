using Terminal.Gui;

namespace RosaDB.TUI
{
    public class NavigationView : FrameView
    {
        public Action<ListViewItemEventArgs>? SelectedItemChanged;

        private readonly ListView listView;

        public NavigationView(List<string> items) : base("Navigation")
        {
            listView = new ListView(items)
            {
                X = 0,
                Y = 0,
                Width = Dim.Fill(),
                Height = Dim.Fill()
            };
            listView.SelectedItemChanged += (args) => SelectedItemChanged?.Invoke(args);
            Add(listView);
        }

        public int SelectedItem {
            get => listView.SelectedItem;
            set => listView.SelectedItem = value;
        }
    }
}
