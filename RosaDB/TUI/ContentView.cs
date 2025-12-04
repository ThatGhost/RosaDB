using Terminal.Gui;

namespace RosaDB.TUI
{
    public class ContentView : FrameView
    {
        private readonly Label contentLabel;

        public ContentView() : base("Content")
        {
            contentLabel = new Label("Select an item from the navigation.")
            {
                X = 1,
                Y = 1,
                Width = Dim.Fill(),
                Height = Dim.Fill()
            };
            Add(contentLabel);
        }

        public void UpdateContent(string newContent)
        {
            contentLabel.Text = newContent;
        }
    }
}
