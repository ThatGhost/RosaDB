using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine;

public class FolderManager : IFolderManager
{
    private readonly IFileSystem _fileSystem;
    public string BasePath { get; }

    public FolderManager(IFileSystem fileSystem)
    {
        _fileSystem = fileSystem;
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        BasePath = _fileSystem.Path.Combine(localAppData, "RosaDB");

        if (!_fileSystem.Directory.Exists(BasePath))
        {
            _fileSystem.Directory.CreateDirectory(BasePath);
        }
    }

    public Task CreateFolder(string folderName)
    {
        var folderPath = _fileSystem.Path.Combine(BasePath, folderName);
        if (!_fileSystem.Directory.Exists(folderPath))
        {
            _fileSystem.Directory.CreateDirectory(folderPath);
        }
        return Task.CompletedTask;
    }

    public Task DeleteFolder(string folderName)
    {
        var folderPath = _fileSystem.Path.Combine(BasePath, folderName);
        if (_fileSystem.Directory.Exists(folderPath))
        {
            _fileSystem.Directory.Delete(folderPath, true);
        }
        return Task.CompletedTask;
    }

    public Task<bool> DoesFolderExist(string folderName)
    {
        var folderPath = _fileSystem.Path.Combine(BasePath, folderName);
        return Task.FromResult(_fileSystem.Directory.Exists(folderPath));
    }

    public Task RenameFolder(string oldName, string newName)
    {
        var oldPath = _fileSystem.Path.Combine(BasePath, oldName);
        var newPath = _fileSystem.Path.Combine(BasePath, newName);

        if (_fileSystem.Directory.Exists(oldPath))
        {
            _fileSystem.Directory.Move(oldPath, newPath);
        }
        return Task.CompletedTask;
    }
}