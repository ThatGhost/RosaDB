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
    public class CellManagerTests
    {
        private Mock<SessionState> _mockSessionState;
        private Mock<IFolderManager> _mockFolderManager;
        private MockFileSystem _mockFileSystem;
        private CellManager _cellManager;

        private const string TestDbName = "TestDB";
        private const string TestCellName = "TestCell";
        private string _basePath = @"C:\rosadb";
        private string _cellPath;
        private string _envFilePath;

        [SetUp]
        public void Setup()
        {
            _mockSessionState = new Mock<SessionState>();
            _mockFolderManager = new Mock<IFolderManager>();
            _mockFileSystem = new MockFileSystem();

            _mockFolderManager.Setup(fm => fm.BasePath).Returns(_basePath);
            var dbResult = Database.Create(TestDbName);
            if(dbResult.IsSuccess)
                _mockSessionState.Setup(ss => ss.CurrentDatabase).Returns(dbResult.Value);

            _cellPath = _mockFileSystem.Path.Combine(_basePath, TestDbName, TestCellName);
            _envFilePath = _mockFileSystem.Path.Combine(_cellPath, "_env");

            _cellManager = new CellManager(
                _mockSessionState.Object,
                _mockFileSystem,
                _mockFolderManager.Object
            );

            // Ensure the base DB directory exists for tests
            _mockFileSystem.AddDirectory(_mockFileSystem.Path.Combine(_basePath, TestDbName));
        }

        private void CreateFakeCellEnvironmentFile(CellEnvironment env)
        {
            if (!_mockFileSystem.Directory.Exists(_cellPath))
                _mockFileSystem.AddDirectory(_cellPath);
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            _mockFileSystem.AddFile(_envFilePath, new MockFileData(bytes));
        }

        [Test]
        public async Task CreateCellEnvironment_HappyPath_CreatesEnvFile()
        {
            // Arrange
            var columns = new List<Column> { Column.Create("Id", DataType.INT).Value };
            if (!_mockFileSystem.Directory.Exists(_cellPath))
                _mockFileSystem.AddDirectory(_cellPath);

            // Act
            var result = await _cellManager.CreateCellEnvironment(TestCellName, columns);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(_mockFileSystem.File.Exists(_envFilePath), Is.True);
            var bytes = _mockFileSystem.File.ReadAllBytes(_envFilePath);
            var env = ByteObjectConverter.ByteArrayToObject<CellEnvironment>(bytes);
            Assert.That(env.Columns.Length, Is.EqualTo(1));
            Assert.That(env.Columns[0].Name, Is.EqualTo("Id"));
        }

        [Test]
        public async Task AddTables_HappyPath_AddsTableToEnvironment()
        {
            // Arrange
            var initialEnv = new CellEnvironment { Columns = Array.Empty<Column>(), Tables = Array.Empty<Table>() };
            CreateFakeCellEnvironmentFile(initialEnv);
            var newTableResult = Table.Create("NewTable", new[] { Column.Create("Col1", DataType.VARCHAR).Value });
            Assert.That(newTableResult.IsSuccess, Is.True);
            var newTables = new[] { newTableResult.Value };

            // Act
            var result = await _cellManager.AddTables(TestCellName, newTables);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var bytes = _mockFileSystem.File.ReadAllBytes(_envFilePath);
            var updatedEnv = ByteObjectConverter.ByteArrayToObject<CellEnvironment>(bytes);
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
            var initialEnv = new CellEnvironment { Columns = Array.Empty<Column>(), Tables = new[] { table } };
            CreateFakeCellEnvironmentFile(initialEnv);

            // Act
            var result = await _cellManager.DeleteTable(TestCellName, tableName);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var bytes = _mockFileSystem.File.ReadAllBytes(_envFilePath);
            var updatedEnv = ByteObjectConverter.ByteArrayToObject<CellEnvironment>(bytes);
            Assert.That(updatedEnv.Tables, Is.Empty);

            string tablePath = _mockFileSystem.Path.Combine(_basePath, TestDbName, TestCellName, tableName);
            string trashPath = _mockFileSystem.Path.Combine(_basePath, TestDbName, TestCellName, "trash_" + tableName);
            _mockFolderManager.Verify(fm => fm.RenameFolder(tablePath, trashPath), Times.Once);
            _mockFolderManager.Verify(fm => fm.DeleteFolder(trashPath), Times.Once);
        }

        [Test]
        public async Task DeleteTable_TableNotFound_ReturnsError()
        {
            // Arrange
            var initialEnv = new CellEnvironment { Columns = Array.Empty<Column>(), Tables = Array.Empty<Table>() };
            CreateFakeCellEnvironmentFile(initialEnv);

            // Act
            var result = await _cellManager.DeleteTable(TestCellName, "NonExistentTable");

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Does.Contain("not found"));
        }

        [Test]
        public async Task GetEnvironment_ReadsFromFile_WhenNotInCache()
        {
            // Arrange
            var env = new CellEnvironment { Columns = new[] { Column.Create("Id", DataType.INT).Value }, Tables = Array.Empty<Table>() };
            CreateFakeCellEnvironmentFile(env);

            // Act
            var result = await _cellManager.GetEnvironment(TestCellName);

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
            var env = new CellEnvironment { Columns = Array.Empty<Column>(), Tables = new[] { table } };
            CreateFakeCellEnvironmentFile(env);
            
            // Act
            var result = await _cellManager.GetColumnsFromTable(TestCellName, tableName);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.Length, Is.EqualTo(1));
            Assert.That(result.Value[0].Name, Is.EqualTo("ColA"));
        }
    }
}
