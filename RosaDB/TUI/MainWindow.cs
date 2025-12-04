using System.Collections.Generic;
using Terminal.Gui;

namespace RosaDB.TUI
{
    public class MainWindow : Window
    {
        private readonly FrameView contentView;
        private readonly List<string> navigationItems;

        private readonly HomeView homeView;
        private readonly QueryView queryView;

        public MainWindow() : base("RosaDB")
        {
            X = 0;
            Y = 1; // For menu
            Width = Dim.Fill();
            Height = Dim.Fill();

            navigationItems = ["Home", "Logs", "Tables", "Query", "Settings"];

            var navigationView1 = new NavigationView(navigationItems)
            {
                X = 0,
                Y = 0,
                Width = 20,
                Height = Dim.Fill()
            };

            contentView = new FrameView("Content")
            {
                X = 20,
                Y = 0,
                Width = Dim.Fill(),
                Height = Dim.Fill()
            };
            
            homeView = new HomeView();
            queryView = new QueryView();

            navigationView1.SelectedItemChanged += OnNavigationItemSelected;

            Add(navigationView1, contentView);

            navigationView1.SelectedItem = 0;
            OnNavigationItemSelected(new ListViewItemEventArgs(0, navigationItems[0]));
        }

        private void OnNavigationItemSelected(ListViewItemEventArgs args)
        {
            var selectedItem = navigationItems[args.Item];
            contentView.RemoveAll();
            contentView.Title = selectedItem;

            switch (selectedItem)
            {
                case "Home":
                    contentView.Add(homeView);
                    break;
                case "Query":
                    contentView.Add(queryView);
                    break;
                default:
                    var defaultLabel = new Label($"View for {selectedItem}")
                    {
                        X = 1,
                        Y = 1
                    };
                    contentView.Add(defaultLabel);
                    break;
            }
        }
    }
}
