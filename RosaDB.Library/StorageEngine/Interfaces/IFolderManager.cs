namespace RosaDB.Library.StorageEngine.Interfaces;

public interface IFolderManager
{
    public string BasePath { get; }
    public void CreateFolder(string folderName);
    public void DeleteFolder(string folderName);
    public bool DoesFolderExist(string folderName);
    public void RenameFolder(string oldName, string newName);
}
