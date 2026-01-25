#nullable disable

using System.IO.Abstractions;
using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Tests.StorageEngine;

[TestFixture]
public class ContextManagerTests
{
    private Mock<SessionState> _sessionStateMock;
    private Mock<IFileSystem> _fileSystemMock;
    private Mock<IFolderManager> _folderManagerMock;
    private Mock<IIndexManager> _indexManagerMock;
    
    private ContextManager _contextManager;
    
    private readonly string _contextName = "test";
    private readonly Column[] _columns = [
        Column.Create("id", DataType.BIGINT, isIndex: true).Value,
        Column.Create("name", DataType.TEXT).Value,
        Column.Create("createdAt", DataType.VARCHAR).Value
    ];

    private Row _instanceRow;
    
    private ContextEnvironment _mockEnviroument;
    
    [SetUp]
    public void Setup()
    {
        _fileSystemMock = new Mock<IFileSystem>();
        _folderManagerMock = new Mock<IFolderManager>();
        _indexManagerMock = new Mock<IIndexManager>();
        _sessionStateMock = new Mock<SessionState>();
        _contextManager = new ContextManager(_sessionStateMock.Object, _fileSystemMock.Object, _folderManagerMock.Object, _indexManagerMock.Object);
        
        _mockEnviroument = new ContextEnvironment()
        {
            Version = 1,
            Columns = _columns,
            Tables = []
        };
        _instanceRow = Row.Create([(long) 5, "name", DateTime.Now.ToShortDateString()], _columns).Value;
        Utils.SetupSessionState(_sessionStateMock);
    }

    [Test]
    public async Task CreateContextEnvironment_ShouldSucceed()
    {
        // Arrange
        SetupContextFilePath();
        
        // Act
        var result = await _contextManager.CreateContextEnvironment(_contextName, _columns);
        
        // Assert
        Utils.AssertSuccess(result);
        _fileSystemMock.Verify(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, It.IsAny<string>()), Times.Once);
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<Byte[]>(), CancellationToken.None), Times.Once);
    }
    
    [Test]
    public async Task CreateContextEnvironment_NoDatabaseSet()
    {
        // Arrange
        SetupContextFilePath();
        _sessionStateMock.Setup(s => s.CurrentDatabase).Returns((Database)null);
        
        // Act
        var result = await _contextManager.CreateContextEnvironment(_contextName, _columns);
        
        // Assert
        Utils.AssertFailure(result, new DatabaseNotSetError());
        _fileSystemMock.Verify(f => f.Path.Combine(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(It.IsAny<string>(), It.IsAny<Byte[]>(), CancellationToken.None), Times.Never);
    }

    [Test]
    public async Task UpdateContextEnvironment_ShouldSucceed()
    {
        // Arrange 
        SetupContextFilePath();
        SetupContextEnviroument();
        
        // Act
        var result = await _contextManager.UpdateContextEnvironment(_contextName, _columns);
        
        // Assert
        Utils.AssertSuccess(result);
    }

    [Test]
    public async Task UpdateContextEnvironment_NoEnviroumentSet()
    {
        // Arrange 
        SetupContextFilePath();
        
        // Act
        var result = await _contextManager.UpdateContextEnvironment(_contextName, _columns);
        
        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.FileError, "Context Environment does not exist"));
    }

    [Test]
    public void CreateContextInstance_ShouldSucceed()
    {
        // Arrange 
        SetupContextFilePath();
        SetupContextEnviroument();
        Row instanceRow = Row.Create([(long) 5, "name", DateTime.Now.ToShortDateString()], _columns).Value;
        string instanceHash = "instanceHash";
        _indexManagerMock.Setup(i => i.ContextDataExists(_contextName, It.IsAny<byte[]>())).Returns(Result<bool>.Success(false));
        _indexManagerMock.Setup(i => i.InsertContextData(_contextName, It.IsAny<byte[]>(), It.IsAny<byte[]>())).Returns(Result.Success());
        _indexManagerMock.Setup(i => i.InsertContextPropertyIndex(_contextName, "id", It.IsAny<byte[]>(), It.IsAny<byte[]>())).Returns(Result.Success());
        
        // Act
        var result = _contextManager.CreateContextInstance(_contextName, instanceHash, instanceRow, _columns);

        // Assert
        Utils.AssertSuccess(result);
    }
    
    [Test]
    public void CreateContextInstance_ShouldFailWhenInstanceAlreadyExists()
    {
        // Arrange 
        SetupContextFilePath();
        SetupContextEnviroument();
        string instanceHash = "instanceHash";
        _indexManagerMock.Setup(i => i.ContextDataExists(_contextName, It.IsAny<byte[]>())).Returns(Result<bool>.Success(true));
        
        // Act
        var result = _contextManager.CreateContextInstance(_contextName, instanceHash, _instanceRow, _columns);

        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.DataError, "Context instance already exists"));
    }

    [Test]
    public async Task GetContextInstance_ShouldSucceed()
    {
        // Arrange
        SetupContextFilePath();
        SetupContextEnviroument();
        string instanceHash = "instanceHash";
        _indexManagerMock.Setup(i => i.GetContextData(_contextName, It.IsAny<byte[]>())).Returns(Result<byte[]>.Success(RowSerializer.Serialize(_instanceRow).Value));
        
        // Act
        var result = await _contextManager.GetContextInstance(_contextName, instanceHash);
        
        // Assert
        Utils.AssertSuccess(result);
    }

    [Test]
    public async Task GetAllContextInstances_ShouldSucceed()
    {
        // Arrange
        SetupContextFilePath();
        SetupContextEnviroument();
        _indexManagerMock.Setup(i => i.GetAllContextData(_contextName)).Returns(Result<IEnumerable<KeyValuePair<byte[], byte[]>>>.Success(new List<KeyValuePair<byte[], byte[]>>()
        {
            new([], RowSerializer.Serialize(_instanceRow).Value)
        }));
        
        // Act
        var result = await _contextManager.GetAllContextInstances(_contextName);
        
        // Assert
        Utils.AssertSuccess(result);
    }

    [Test]
    public async Task CreateTable_ShouldSucceed()
    {
        // Arrange
        SetupContextFilePath();
        SetupContextEnviroument();
        Table table = Table.Create("table", _columns).Value;
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, "table")).Returns(FilePath);
        _fileSystemMock.Setup(f => f.Directory.Exists(FilePath)).Returns(false);
        
        // Act
        var result = await _contextManager.CreateTable(_contextName, table);

        // Assert
        Utils.AssertSuccess(result);
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()), Times.Once);
        _fileSystemMock.Verify(f => f.Directory.CreateDirectory(FilePath), Times.Once);
    }
    
    [Test]
    public async Task CreateTable_ShouldSucceed_FolderAlreadyExists()
    {
        // Arrange
        SetupContextFilePath();
        SetupContextEnviroument();
        Table table = Table.Create("table", _columns).Value;
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, "table")).Returns(FilePath);
        _fileSystemMock.Setup(f => f.Directory.Exists(FilePath)).Returns(true);
        
        // Act
        var result = await _contextManager.CreateTable(_contextName, table);

        // Assert
        Utils.AssertSuccess(result);
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()));
        _fileSystemMock.Verify(f => f.Directory.CreateDirectory(FilePath), Times.Never);
    }
    
    [Test]
    public async Task CreateTable_ShouldFail_CreateFolderFailed()
    {
        // Arrange
        SetupContextFilePath();
        SetupContextEnviroument();
        Table table = Table.Create("table", _columns).Value;
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, "table")).Returns(FilePath);
        _fileSystemMock.Setup(f => f.Directory.Exists(FilePath)).Returns(false);
        _fileSystemMock.Setup(f => f.Directory.CreateDirectory(FilePath)).Throws(new Exception());
        
        // Act
        var result = await _contextManager.CreateTable(_contextName, table);

        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.FileError, $"Failed to create directory for table"));
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        _fileSystemMock.Verify(f => f.Directory.CreateDirectory(FilePath), Times.Once);
    }
    
    [Test]
    public async Task DeleteTable_ShouldSucceed()
    {
        // Arrange
        SetupContextFilePath();
        string tableName = "table";
        Table table = Table.Create(tableName, _columns).Value;
        SetupContextEnviroument(new ContextEnvironment()
        {
            Version = 1,
            Columns = _columns,
            Tables = [table]
        });
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, tableName)).Returns(FilePath);
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, "trash_"+tableName)).Returns(FilePath + "trash");
        
        
        // Act
        var result = await _contextManager.DeleteTable(_contextName, tableName);

        // Assert
        Utils.AssertSuccess(result);
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()), Times.Once);
        _folderManagerMock.Verify(f => f.DeleteFolder(FilePath + "trash"), Times.Once);
    }
    
    [Test]
    public async Task DeleteTable_ShouldFail_NoSuchTable()
    {
        // Arrange
        SetupContextFilePath();
        string tableName = "table";
        SetupContextEnviroument();
        
        // Act
        var result = await _contextManager.DeleteTable(_contextName, tableName);

        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.DataError, $"Table '{tableName}' not found in context '{_contextName}'."));
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()), Times.Never);
        _folderManagerMock.Verify(f => f.DeleteFolder(FilePath + "trash"), Times.Never);
    }
    
    [Test]
    public async Task DeleteTable_ShouldFail_RenameFailed()
    {
        // Arrange
        SetupContextFilePath();
        string tableName = "table";
        Table table = Table.Create(tableName, _columns).Value;
        SetupContextEnviroument(new ContextEnvironment()
        {
            Version = 1,
            Columns = _columns,
            Tables = [table]
        });
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, tableName)).Returns(FilePath);
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, "trash_"+tableName)).Returns(FilePath + "trash");
        _folderManagerMock.Setup(f => f.RenameFolder(FilePath, FilePath+"trash")).Throws(new Exception());
        
        // Act
        var result = await _contextManager.DeleteTable(_contextName, tableName);

        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.FileError, "Could not prepare table for deletion (Folder Rename Failed)."));
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()), Times.Never);
        _folderManagerMock.Verify(f => f.DeleteFolder(FilePath + "trash"), Times.Never);
    }
    
    [Test]
    public async Task DeleteTable_ShouldFail_FailedToRevert()
    {
        // Arrange
        SetupContextFilePath();
        string tableName = "table";
        string tableNameTrash = "table_trash";
        Table table = Table.Create(tableName, _columns).Value;
        SetupContextEnviroument(new ContextEnvironment()
        {
            Version = 1,
            Columns = _columns,
            Tables = [table]
        });
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, tableName)).Returns(FilePath);
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, "trash_"+tableName)).Returns(tableNameTrash);
        _fileSystemMock.Setup(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<byte[]>(), It.IsAny<CancellationToken>())).Throws(new Exception());
        _folderManagerMock.Setup(f => f.RenameFolder(tableNameTrash, FilePath)).Throws(new Exception());
        
        // Act
        var result = await _contextManager.DeleteTable(_contextName, tableName);

        // Assert
        Utils.AssertFailure(result, new CriticalError());
        _folderManagerMock.Verify(f => f.DeleteFolder(tableNameTrash), Times.Never);
    }
    
    [Test]
    public async Task DeleteTable_ShouldFail_FailedWriteButSuccessfullRevert()
    {
        // Arrange
        SetupContextFilePath();
        string tableName = "table";
        string tableNameTrash = "table_trash";
        Table table = Table.Create(tableName, _columns).Value;
        SetupContextEnviroument(new ContextEnvironment()
        {
            Version = 1,
            Columns = _columns,
            Tables = [table]
        });
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, tableName)).Returns(FilePath);
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, "trash_"+tableName)).Returns(tableNameTrash);
        _fileSystemMock.Setup(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<byte[]>(), It.IsAny<CancellationToken>())).Throws(new Exception());
        
        // Act
        var result = await _contextManager.DeleteTable(_contextName, tableName);

        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.FileError, "Failed to update context definition. Deletion reverted."));
        _folderManagerMock.Verify(f => f.DeleteFolder(tableNameTrash), Times.Never);
    }
    
    [Test]
    public async Task DeleteTable_ShouldSucceed_WhenRenameFails()
    {
        // Arrange
        SetupContextFilePath();
        string tableName = "table";
        Table table = Table.Create(tableName, _columns).Value;
        SetupContextEnviroument(new ContextEnvironment()
        {
            Version = 1,
            Columns = _columns,
            Tables = [table]
        });
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, tableName)).Returns(FilePath);
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, "trash_"+tableName)).Returns(FilePath + "trash");
        _folderManagerMock.Setup(f => f.DeleteFolder(FilePath+"trash")).Throws(new Exception());
        
        // Act
        var result = await _contextManager.DeleteTable(_contextName, tableName);

        // Assert
        Utils.AssertSuccess(result);
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Test]
    public async Task GetEnvironment_ShouldSucceed()
    {
        // Arrange
        SetupContextFilePath();
        SetupContextEnviroument();
        
        // Act
        var result = await _contextManager.GetEnvironment(_contextName);
        
        // Assert
        Utils.AssertSuccess(result);
    }
    
    [Test]
    public async Task GetEnvironment_ShouldFail_DatabaseNotSet()
    {
        // Arrange
        SetupContextFilePath();
        _sessionStateMock.Setup(s => s.CurrentDatabase).Returns((Database)null);
        
        // Act
        var result = await _contextManager.GetEnvironment(_contextName);
        
        // Assert
        Utils.AssertFailure(result, new DatabaseNotSetError());
        _fileSystemMock.Verify(f => f.File.ReadAllBytesAsync(FilePath, It.IsAny<CancellationToken>()), Times.Never);
    }
    
    [Test]
    public async Task GetEnvironment_ShouldFail_FileDoesNotExist()
    {
        // Arrange
        SetupContextFilePath();
        _fileSystemMock.Setup(f => f.File.Exists(FilePath)).Returns(false);
        
        // Act
        var result = await _contextManager.GetEnvironment(_contextName);
        
        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.FileError, "Context Environment does not exist"));
        _fileSystemMock.Verify(f => f.File.ReadAllBytesAsync(FilePath, It.IsAny<CancellationToken>()), Times.Never);
    }
    
    [Test]
    public async Task GetEnvironment_ShouldFail_ReadReturnsEmptyByteArray()
    {
        // Arrange
        SetupContextFilePath();
        _fileSystemMock.Setup(f => f.File.Exists(FilePath)).Returns(true);
        _fileSystemMock.Setup(f => f.File.ReadAllBytesAsync(FilePath, It.IsAny<CancellationToken>())).Returns(Task.FromResult<byte[]>([]));
        
        // Act
        var result = await _contextManager.GetEnvironment(_contextName);
        
        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.FileError, "Context Environment does not exist"));
    }
    
    [Test]
    public async Task GetEnvironment_ShouldFail_MalformedObject()
    {
        // Arrange
        SetupContextFilePath();
        _fileSystemMock.Setup(f => f.File.Exists(FilePath)).Returns(true);
        _fileSystemMock.Setup(f => f.File.ReadAllBytesAsync(FilePath, It.IsAny<CancellationToken>())).Returns(Task.FromResult<byte[]>([0x00,0x01]));
        
        // Act
        var result = await _contextManager.GetEnvironment(_contextName);
        
        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.FileError, "Context Environment does not exist"));
        _fileSystemMock.Verify(f => f.File.ReadAllBytesAsync(FilePath, It.IsAny<CancellationToken>()), Times.Once);
    }
    
    [Test]
    public async Task GetEnvironment_ShouldSucceed_FromCache()
    {
        // Arrange
        SetupContextFilePath();
        SetupContextEnviroument();
        
        // Act
        await _contextManager.GetEnvironment(_contextName);
        var result = await _contextManager.GetEnvironment(_contextName);
        
        // Assert
        Utils.AssertSuccess(result);
        _fileSystemMock.Verify(f => f.File.ReadAllBytesAsync(FilePath, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Test]
    public async Task GetColumnsFromTable_ShouldSucceed()
    {
        // Arrange
        SetupContextFilePath();
        string tableName = "table";
        Table table = Table.Create(tableName, _columns).Value;
        SetupContextEnviroument(new ContextEnvironment()
        {
            Version = 1,
            Columns = _columns,
            Tables = [table]
        });
        
        // Act
        var result = await _contextManager.GetColumnsFromTable(_contextName, tableName);
        
        // Assert
        Utils.AssertSuccess(result);
        Assert.That(result.Value, Is.Not.Null);
        Assert.That(result.Value.Length, Is.EqualTo(3));
        Assert.That(result.Value[0].Name, Is.EqualTo(_columns[0].Name));
    }
    
    [Test]
    public async Task GetColumnsFromTable_ShouldFail_NoSuchTable()
    {
        // Arrange
        SetupContextFilePath();
        SetupContextEnviroument();
        
        // Act
        var result = await _contextManager.GetColumnsFromTable(_contextName, "Table Does Not Exist");
        
        // Assert
        Utils.AssertFailure(result, new Error(ErrorPrefixes.StateError, "Table does not exist in context environment"));
    }

    private readonly string basePath = "basepath";
    private readonly string FilePath = "completeFilePath";
    private void SetupContextFilePath()
    {
        _folderManagerMock.Setup(f => f.BasePath).Returns(basePath);
        _fileSystemMock.Setup(f => f.File.WriteAllBytesAsync(FilePath, It.IsAny<Byte[]>(), CancellationToken.None)).Returns(Task.CompletedTask);
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, It.IsAny<string>())).Returns(FilePath);
    }

    private void SetupContextEnviroument(ContextEnvironment env = null)
    {
        SetupContextFilePath();
        _fileSystemMock.Setup(f => f.File.Exists(It.IsAny<string>())).Returns(true);
        _fileSystemMock.Setup(f => f.File.ReadAllBytesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.FromResult(ByteObjectConverter.ObjectToByteArray(env ?? _mockEnviroument)));
    }
}
