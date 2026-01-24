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
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(ContextFilePath, It.IsAny<Byte[]>(), CancellationToken.None), Times.Once);
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

    private readonly string basePath = "basepath";
    private readonly string ContextFilePath = "completeFilePath";
    private void SetupContextFilePath()
    {
        _folderManagerMock.Setup(f => f.BasePath).Returns(basePath);
        _fileSystemMock.Setup(f => f.File.WriteAllBytesAsync(ContextFilePath, It.IsAny<Byte[]>(), CancellationToken.None)).Returns(Task.CompletedTask);
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, It.IsAny<string>())).Returns(ContextFilePath);
    }

    private void SetupContextEnviroument()
    {
        SetupContextFilePath();
        _fileSystemMock.Setup(f => f.File.Exists(It.IsAny<string>())).Returns(true);
        _fileSystemMock.Setup(f => f.File.ReadAllBytesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.FromResult(ByteObjectConverter.ObjectToByteArray(_mockEnviroument)));
    }
}
