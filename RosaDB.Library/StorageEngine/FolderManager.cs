using System;
using System.IO;
using System.Threading.Tasks;

namespace RosaDB.Library.StorageEngine;

public static class FolderManager
{
    public static string BasePath { get; }

    static FolderManager()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        BasePath = Path.Combine(localAppData, "RosaDB");

        if (!Directory.Exists(BasePath))
        {
            Directory.CreateDirectory(BasePath);
        }
    }

    public static Task CreateFolder(string folderName)
    {
        var folderPath = Path.Combine(BasePath, folderName);
        if (!Directory.Exists(folderPath))
        {
            Directory.CreateDirectory(folderPath);
        }
        return Task.CompletedTask;
    }
}