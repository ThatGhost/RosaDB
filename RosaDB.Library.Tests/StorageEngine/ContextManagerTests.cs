#nullable disable

using System.IO.Abstractions;
using Moq;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;

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
    
    [SetUp]
    public void Setup()
    {
        _fileSystemMock = new Mock<IFileSystem>();
        _folderManagerMock = new Mock<IFolderManager>();
        _indexManagerMock = new Mock<IIndexManager>();
        _sessionStateMock = new Mock<SessionState>();
        _contextManager = new ContextManager(_sessionStateMock.Object, _fileSystemMock.Object, _folderManagerMock.Object, _indexManagerMock.Object);
        
        Utils.SetupContext(_sessionStateMock);
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
        Utils.AssertSuccess(result);
        _fileSystemMock.Verify(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, It.IsAny<string>()), Times.Once);
        _fileSystemMock.Verify(f => f.File.WriteAllBytesAsync(ContextFilePath, It.IsAny<Byte[]>(), CancellationToken.None), Times.Once);
    }

    private readonly string basePath = "basepath";
    private readonly string ContextFilePath = "completeFilePath";
    private void SetupContextFilePath()
    {
        _folderManagerMock.Setup(f => f.BasePath).Returns(basePath);
        _fileSystemMock.Setup(f => f.File.WriteAllBytesAsync(ContextFilePath, It.IsAny<Byte[]>(), CancellationToken.None)).Returns(Task.CompletedTask);
        _fileSystemMock.Setup(f => f.Path.Combine(basePath, Utils.DatabaseName, _contextName, It.IsAny<string>())).Returns(ContextFilePath);
    }
}
