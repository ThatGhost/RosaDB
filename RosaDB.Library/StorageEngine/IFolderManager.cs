namespace RosaDB.Library.StorageEngine;

public interface IFolderManager
{
    string BasePath { get; }
    Task CreateFolder(string folderName);
    Task DeleteFolder(string folderName);
    Task<bool> DoesFolderExist(string folderName);
    Task RenameFolder(string oldName, string newName);
}
