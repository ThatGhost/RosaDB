using Terminal.Gui;
using RosaDB.TUI;

namespace RosaDB;

class Program
{
    static void Main(string[] args)
    {
        Application.Init();
        
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
