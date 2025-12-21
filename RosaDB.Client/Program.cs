using Terminal.Gui;
using RosaDB.Client.TUI;

namespace RosaDB.Client;

class Program
{
    static void Main(string[] args)
    {
        Application.Init();
        
        Colors.Base.Normal = Application.Driver.MakeAttribute(Color.White, Color.Black);
        
        var top = Application.Top;
        
        var mainWindow = new MainWindow();

        var menu = new MenuBar([
            new MenuBarItem("_File", [
                new MenuItem("_Quit", "", () => Application.RequestStop())
            ]),
            new MenuBarItem("_Help", [new MenuItem("_About", "", () =>
                {
                    MessageBox.Query("About RosaDB", "A simple TUI database client.", "Ok");
                })
            ])
        ]);

        top.Add(mainWindow, menu);
        
        Application.Run();
        Application.Shutdown();
    }
}
