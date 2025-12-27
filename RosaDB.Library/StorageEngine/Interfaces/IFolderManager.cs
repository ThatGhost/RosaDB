namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IFolderManager
{
    string BasePath { get; }
    void CreateFolder(string folderName);
    void DeleteFolder(string folderName);
    bool DoesFolderExist(string folderName);
    void RenameFolder(string oldName, string newName);
}
