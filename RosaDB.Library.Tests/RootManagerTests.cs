#nullable disable

using Moq;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions.TestingHelpers;
using RosaDB.Library.Core;

namespace RosaDB.Library.Tests
{
    [TestFixture]
    public class RootManagerTests
    {
        private Mock<IDatabaseManager> _mockDatabaseManager;
        private Mock<SessionState> _mockSessionState;
        private Mock<IFolderManager> _mockFolderManager;
        private MockFileSystem _mockFileSystem;
        private RootManager _rootManager;
        private string _envFilePath;

        [SetUp]
        public void Setup()
        {
            _mockDatabaseManager = new Mock<IDatabaseManager>();
            _mockSessionState = new Mock<SessionState>();
            _mockFolderManager = new Mock<IFolderManager>();
            _mockFileSystem = new MockFileSystem();

            string basePath = @"C:\rosadb";
            _mockFolderManager.Setup(fm => fm.BasePath).Returns(basePath);
            _envFilePath = _mockFileSystem.Path.Combine(basePath, "_env");

            _rootManager = new RootManager(
                _mockDatabaseManager.Object,
                _mockSessionState.Object,
                _mockFileSystem,
                _mockFolderManager.Object
            );
        }

        private void CreateFakeEnvironmentFile(RootEnvironment env)
        {
            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            _mockFileSystem.AddFile(_envFilePath, new MockFileData(bytes));
        }

        [Test]
        public async Task InitializeRoot_NoEnvironmentExists_CreatesRootEnvironmentFile()
        {
            // Arrange
            Assert.That(_mockFileSystem.File.Exists(_envFilePath), Is.False);

            // Act
            var result = await _rootManager.InitializeRoot();

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(_mockFileSystem.File.Exists(_envFilePath), Is.True);
            var bytes = _mockFileSystem.File.ReadAllBytes(_envFilePath);
            var env = ByteObjectConverter.ByteArrayToObject<RootEnvironment>(bytes);
            Assert.That(env.DatabaseNames, Is.Empty);
        }

        [Test]
        public async Task InitializeRoot_EnvironmentAlreadyExists_ReturnsError()
        {
            // Arrange
            CreateFakeEnvironmentFile(new RootEnvironment());

            // Act
            var result = await _rootManager.InitializeRoot();

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Is.EqualTo("Root already setup"));
        }

        [Test]
        public async Task CreateDatabase_HappyPath_CreatesFolderAndUpdatesEnv()
        {
            // Arrange
            var dbName = "TestDB";
            CreateFakeEnvironmentFile(new RootEnvironment());
            _mockDatabaseManager.Setup(dm => dm.CreateDatabaseEnvironment(It.IsAny<Database>()))
                .ReturnsAsync(Result.Success());

            // Act
            var result = await _rootManager.CreateDatabase(dbName);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            _mockFolderManager.Verify(fm => fm.CreateFolder(dbName), Times.Once);
            _mockDatabaseManager.Verify(dm => dm.CreateDatabaseEnvironment(It.Is<Database>(d => d.Name == dbName)), Times.Once);

            var env = ByteObjectConverter.ByteArrayToObject<RootEnvironment>(_mockFileSystem.File.ReadAllBytes(_envFilePath));
            Assert.That(env.DatabaseNames, Contains.Item(dbName));
        }

        [Test]
        public async Task CreateDatabase_DatabaseAlreadyExists_ReturnsError()
        {
            // Arrange
            var dbName = "ExistingDB";
            var env = new RootEnvironment();
            env.DatabaseNames.Add(dbName);
            CreateFakeEnvironmentFile(env);

            // Act
            var result = await _rootManager.CreateDatabase(dbName);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Does.Contain("already exists"));
        }

        [Test]
        public async Task DeleteDatabase_HappyPath_RemovesFromEnvAndDeletesFolder()
        {
            // Arrange
            var dbName = "ToDeleteDB";
            var env = new RootEnvironment();
            env.DatabaseNames.Add(dbName);
            CreateFakeEnvironmentFile(env);

            // Act
            var result = await _rootManager.DeleteDatabase(dbName);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var updatedEnv = ByteObjectConverter.ByteArrayToObject<RootEnvironment>(_mockFileSystem.File.ReadAllBytes(_envFilePath));
            Assert.That(updatedEnv.DatabaseNames, Does.Not.Contain(dbName));

            string folderPath = _mockFileSystem.Path.Combine(_mockFolderManager.Object.BasePath, dbName);
            string trashFolderPath = _mockFileSystem.Path.Combine(_mockFolderManager.Object.BasePath, "trash_" + dbName);
            _mockFolderManager.Verify(fm => fm.RenameFolder(folderPath, trashFolderPath), Times.Once);
            _mockFolderManager.Verify(fm => fm.DeleteFolder(trashFolderPath), Times.Once);
        }

        [Test]
        public async Task DeleteDatabase_DatabaseNotFound_ReturnsError()
        {
            // Arrange
            var dbName = "NonExistentDB";
            CreateFakeEnvironmentFile(new RootEnvironment());

            // Act
            var result = await _rootManager.DeleteDatabase(dbName);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Does.Contain("not found"));
        }

        [Test]
        public async Task GetDatabaseNames_ReturnsListOfDatabases()
        {
            // Arrange
            var env = new RootEnvironment();
            env.DatabaseNames.Add("DB1");
            env.DatabaseNames.Add("DB2");
            CreateFakeEnvironmentFile(env);

            // Act
            var result = await _rootManager.GetDatabaseNames();

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.Count, Is.EqualTo(2));
            Assert.That(result.Value, Contains.Item("DB1"));
        }
    }
}
