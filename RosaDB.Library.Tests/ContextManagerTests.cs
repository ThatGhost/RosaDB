#nullable disable

using Moq;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions.TestingHelpers;

namespace RosaDB.Library.Tests
{
    [TestFixture]
    public class ContextManagerTests
    {
        private Mock<SessionState> _mockSessionState;
        private Mock<IFolderManager> _mockFolderManager;
        private Mock<IIndexManager> _mockIndexManager;
        private MockFileSystem _mockFileSystem;
        private ContextManager _contextManager;

        private const string TestDbName = "TestDB";
        private const string TestContextName = "TestContext";
        private const string _basePath = @"C:\rosadb";
        private string _contextPath;
        private string _envFilePath;

        [SetUp]
        public void Setup()
        {
            _mockSessionState = new Mock<SessionState>();
            _mockFolderManager = new Mock<IFolderManager>();
            _mockFileSystem = new MockFileSystem();
            _mockIndexManager = new Mock<IIndexManager>();

            _mockFolderManager.Setup(fm => fm.BasePath).Returns(_basePath);
            var dbResult = Database.Create(TestDbName);
            if(dbResult.IsSuccess)
                _mockSessionState.Setup(ss => ss.CurrentDatabase).Returns(dbResult.Value);

            _contextPath = _mockFileSystem.Path.Combine(_basePath, TestDbName, TestContextName);
            _envFilePath = _mockFileSystem.Path.Combine(_contextPath, "_env");

            _contextManager = new ContextManager(
                _mockSessionState.Object,
                _mockFileSystem,
                _mockFolderManager.Object,
                _mockIndexManager.Object
            );

            // Ensure the base DB directory exists for tests
            _mockFileSystem.AddDirectory(_mockFileSystem.Path.Combine(_basePath, TestDbName));
        }

        private void CreateFakeContextEnvironmentFile(ContextEnvironment env)
        {
            if (!_mockFileSystem.Directory.Exists(_contextPath))
                _mockFileSystem.AddDirectory(_contextPath);
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            _mockFileSystem.AddFile(_envFilePath, new MockFileData(bytes));
        }

        [Test]
        public async Task CreateContextEnvironment_HappyPath_CreatesEnvFile()
        {
            // Arrange
            var columns = new Column[] { Column.Create("Id", DataType.INT).Value };
            if (!_mockFileSystem.Directory.Exists(_contextPath))
                _mockFileSystem.AddDirectory(_contextPath);

            // Act
            var result = await _contextManager.CreateContextEnvironment(TestContextName, columns);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(_mockFileSystem.File.Exists(_envFilePath), Is.True);
            var bytes = _mockFileSystem.File.ReadAllBytes(_envFilePath);
            var env = ByteObjectConverter.ByteArrayToObject<ContextEnvironment>(bytes);
            Assert.That(env.Columns.Length, Is.EqualTo(1));
            Assert.That(env.Columns[0].Name, Is.EqualTo("Id"));
        }

        [Test]
        public async Task AddTables_HappyPath_AddsTableToEnvironment()
        {
            // Arrange
            var initialEnv = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(initialEnv);
            var newTableResult = Table.Create("NewTable", new[] { Column.Create("Col1", DataType.VARCHAR).Value });
            Assert.That(newTableResult.IsSuccess, Is.True);

            // Act
            var result = await _contextManager.CreateTable(TestContextName, newTableResult.Value);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var bytes = _mockFileSystem.File.ReadAllBytes(_envFilePath);
            var updatedEnv = ByteObjectConverter.ByteArrayToObject<ContextEnvironment>(bytes);
            Assert.That(updatedEnv.Tables.Length, Is.EqualTo(1));
            Assert.That(updatedEnv.Tables[0].Name, Is.EqualTo("NewTable"));
        }

        [Test]
        public async Task DeleteTable_HappyPath_RemovesTableAndFolder()
        {
            // Arrange
            var tableName = "TableToDelete";
            var tableResult = Table.Create(tableName, Array.Empty<Column>());
            Assert.That(tableResult.IsSuccess, Is.True);
            var table = tableResult.Value;
            var initialEnv = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = new[] { table } };
            CreateFakeContextEnvironmentFile(initialEnv);

            // Act
            var result = await _contextManager.DeleteTable(TestContextName, tableName);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var bytes = _mockFileSystem.File.ReadAllBytes(_envFilePath);
            var updatedEnv = ByteObjectConverter.ByteArrayToObject<ContextEnvironment>(bytes);
            Assert.That(updatedEnv.Tables, Is.Empty);

            string tablePath = _mockFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, tableName);
            string trashPath = _mockFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, "trash_" + tableName);
            _mockFolderManager.Verify(fm => fm.RenameFolder(tablePath, trashPath), Times.Once);
            _mockFolderManager.Verify(fm => fm.DeleteFolder(trashPath), Times.Once);
        }

        [Test]
        public async Task DeleteTable_TableNotFound_ReturnsError()
        {
            // Arrange
            var initialEnv = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(initialEnv);

            // Act
            var result = await _contextManager.DeleteTable(TestContextName, "NonExistentTable");

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Does.Contain("not found"));
        }

        [Test]
        public async Task GetEnvironment_ReadsFromFile_WhenNotInCache()
        {
            // Arrange
            var env = new ContextEnvironment { Columns = new[] { Column.Create("Id", DataType.INT).Value }, Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(env);

            // Act
            var result = await _contextManager.GetEnvironment(TestContextName);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.Columns.Length, Is.EqualTo(1));
            Assert.That(result.Value.Columns[0].Name, Is.EqualTo("Id"));
        }

        [Test]
        public async Task GetColumnsFromTable_HappyPath_ReturnsColumns()
        {
            // Arrange
            var tableName = "MyTable";
            var columns = new[] { Column.Create("ColA", DataType.INT).Value };
            var tableResult = Table.Create(tableName, columns);
            Assert.That(tableResult.IsSuccess, Is.True);
            var table = tableResult.Value;
            var env = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = new[] { table } };
            CreateFakeContextEnvironmentFile(env);
            
            // Act
            var result = await _contextManager.GetColumnsFromTable(TestContextName, tableName);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.Length, Is.EqualTo(1));
            Assert.That(result.Value[0].Name, Is.EqualTo("ColA"));
        }
    }
}
