#nullable disable

using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;

namespace RosaDB.Library.Tests
{
    [TestFixture]
    public class ContextManagerTests
    {
        private Mock<SessionState> _mockSessionState;
        private Mock<IFolderManager> _mockFolderManager;
        private Mock<IIndexManager> _mockIndexManager;
        private Mock<IFileSystem> _mockFileSystem; // Changed to Mock<IFileSystem>
        private MockFileSystem _concreteFileSystem; // Added for operations directly on MockFileSystem
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
            _mockFileSystem = new Mock<IFileSystem>(); // Changed to new Mock<IFileSystem>()
            _concreteFileSystem = new MockFileSystem(); // Initialize concrete MockFileSystem
            _mockIndexManager = new Mock<IIndexManager>();

            _mockFolderManager.Setup(fm => fm.BasePath).Returns(_basePath);
            var dbResult = Database.Create(TestDbName);
            if(dbResult.IsSuccess)
                _mockSessionState.Setup(ss => ss.CurrentDatabase).Returns(dbResult.Value);

            _mockFileSystem.Setup(fs => fs.Path).Returns(_concreteFileSystem.Path); // Setup Path property
            _mockFileSystem.Setup(fs => fs.Directory).Returns(_concreteFileSystem.Directory); // Setup Directory property
            _mockFileSystem.Setup(fs => fs.File).Returns(_concreteFileSystem.File); // Setup File property

            _contextPath = _mockFileSystem.Object.Path.Combine(_basePath, TestDbName, TestContextName);
            _envFilePath = _mockFileSystem.Object.Path.Combine(_contextPath, "_env");

            _contextManager = new ContextManager(
                _mockSessionState.Object,
                _mockFileSystem.Object, // Use .Object here
                _mockFolderManager.Object,
                _mockIndexManager.Object
            );

            // Ensure the base DB directory exists for tests
            _concreteFileSystem.AddDirectory(_concreteFileSystem.Path.Combine(_basePath, TestDbName)); // Use concreteFileSystem here
        }

        private void CreateFakeContextEnvironmentFile(ContextEnvironment env)
        {
            // Ensure the directory exists in the concrete file system
            if (!_concreteFileSystem.Directory.Exists(_contextPath))
                _concreteFileSystem.Directory.CreateDirectory(_contextPath); // Use CreateDirectory here

            var bytes = ByteObjectConverter.ObjectToByteArray(env);
            _concreteFileSystem.AddFile(_envFilePath, new MockFileData(bytes));
        }

        [Test]
        public async Task CreateContextEnvironment_HappyPath_CreatesEnvFile()
        {
            // Arrange
            var columns = new Column[] { Column.Create("Id", DataType.INT).Value };
            // Ensure the context path exists for the MockFileSystem
            _concreteFileSystem.AddDirectory(_contextPath);

            // Act
            var result = await _contextManager.CreateContextEnvironment(TestContextName, columns);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(_concreteFileSystem.File.Exists(_envFilePath), Is.True);
            var bytes = _concreteFileSystem.File.ReadAllBytes(_envFilePath);
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
            var bytes = _concreteFileSystem.File.ReadAllBytes(_envFilePath);
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
            var bytes = _concreteFileSystem.File.ReadAllBytes(_envFilePath);
            var updatedEnv = ByteObjectConverter.ByteArrayToObject<ContextEnvironment>(bytes);
            Assert.That(updatedEnv.Tables, Is.Empty);

            string tablePath = _concreteFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, tableName);
            string trashPath = _concreteFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, "trash_" + tableName);
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
            Assert.That(_concreteFileSystem.File.Exists(_envFilePath), Is.True);
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

        [Test]
        public async Task UpdateContextEnvironment_HappyPath_UpdatesEnvFile()
        {
            // Arrange
            var initialColumns = new Column[] { Column.Create("Id", DataType.INT).Value };
            var initialEnv = new ContextEnvironment { Columns = initialColumns, Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(initialEnv);

            var updatedColumns = new Column[] { Column.Create("Id", DataType.INT).Value, Column.Create("Name", DataType.VARCHAR).Value };

            // Act
            var result = await _contextManager.UpdateContextEnvironment(TestContextName, updatedColumns);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var bytes = _concreteFileSystem.File.ReadAllBytes(_envFilePath);
            var env = ByteObjectConverter.ByteArrayToObject<ContextEnvironment>(bytes);
            Assert.That(env.Columns.Length, Is.EqualTo(2));
            Assert.That(env.Columns[1].Name, Is.EqualTo("Name"));
        }

        [Test]
        public async Task UpdateContextEnvironment_EnvNotFound_ReturnsError()
        {
            // Arrange
            var updatedColumns = new Column[] { Column.Create("Id", DataType.INT).Value };

            // Act
            var result = await _contextManager.UpdateContextEnvironment("NonExistentContext", updatedColumns);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Is.EqualTo("Context Environment does not exist"));
        }

        [Test]
        public async Task CreateContextInstance_HappyPath_InsertsDataAndIndex()
        {
            // Arrange
            var columns = new[] { Column.Create("Id", DataType.INT, isPrimaryKey: true).Value, Column.Create("Name", DataType.VARCHAR).Value };
            var row = Row.Create(new object[] { 1, "Test" }, columns).Value;
            var instanceHash = "hash1";
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);

            _mockIndexManager.Setup(im => im.ContextDataExists(TestContextName, hashBytes)).Returns(Result<bool>.Success(false));
            _mockIndexManager.Setup(im => im.InsertContextData(TestContextName, hashBytes, It.IsAny<byte[]>())).Returns(Result.Success());
            _mockIndexManager.Setup(im => im.InsertContextPropertyIndex(TestContextName, "Id", It.IsAny<byte[]>(), hashBytes)).Returns(Result.Success());

            // Act
            var result = await _contextManager.CreateContextInstance(TestContextName, instanceHash, row, columns);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            _mockIndexManager.Verify(im => im.InsertContextData(TestContextName, hashBytes, It.IsAny<byte[]>()), Times.Once);
            _mockIndexManager.Verify(im => im.InsertContextPropertyIndex(TestContextName, "Id", It.IsAny<byte[]>(), hashBytes), Times.Once);
        }

        [Test]
        public async Task CreateContextInstance_InstanceExists_ReturnsError()
        {
            // Arrange
            var columns = new[] { Column.Create("Id", DataType.INT).Value };
            var row = Row.Create(new object[] { 1 }, columns).Value;
            var instanceHash = "hash1";
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);

            _mockIndexManager.Setup(im => im.ContextDataExists(TestContextName, hashBytes)).Returns(Result<bool>.Success(true));

            // Act
            var result = await _contextManager.CreateContextInstance(TestContextName, instanceHash, row, columns);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Is.EqualTo("Context instance already exists"));
        }

        [Test]
        public async Task CreateContextInstance_InsertDataFails_ReturnsError()
        {
            // Arrange
            var columns = new[] { Column.Create("Id", DataType.INT).Value };
            var row = Row.Create(new object[] { 1 }, columns).Value;
            var instanceHash = "hash1";
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);
            var error = new Error(ErrorPrefixes.FileError, "Insert failed");

            _mockIndexManager.Setup(im => im.ContextDataExists(TestContextName, hashBytes)).Returns(Result<bool>.Success(false));
            _mockIndexManager.Setup(im => im.InsertContextData(TestContextName, hashBytes, It.IsAny<byte[]>())).Returns(error);

            // Act
            var result = await _contextManager.CreateContextInstance(TestContextName, instanceHash, row, columns);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error, Is.EqualTo(error));
        }

        [Test]
        public async Task CreateContextInstance_InsertIndexFails_ReturnsError()
        {
            // Arrange
            var columns = new[] { Column.Create("Id", DataType.INT, isPrimaryKey: true).Value };
            var row = Row.Create(new object[] { 1 }, columns).Value;
            var instanceHash = "hash1";
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);
            var error = new Error(ErrorPrefixes.FileError, "Index failed");

            _mockIndexManager.Setup(im => im.ContextDataExists(TestContextName, hashBytes)).Returns(Result<bool>.Success(false));
            _mockIndexManager.Setup(im => im.InsertContextData(TestContextName, hashBytes, It.IsAny<byte[]>())).Returns(Result.Success());
            _mockIndexManager.Setup(im => im.InsertContextPropertyIndex(TestContextName, "Id", It.IsAny<byte[]>(), hashBytes)).Returns(error);

            // Act
            var result = await _contextManager.CreateContextInstance(TestContextName, instanceHash, row, columns);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error, Is.EqualTo(error));
        }

        [Test]
        public async Task GetContextInstance_HappyPath_ReturnsRow()
        {
            // Arrange
            var instanceHash = "hash1";
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);
            var columns = new[] { Column.Create("Id", DataType.INT).Value };
            var row = Row.Create(new object[] { 123 }, columns).Value;
            var rowBytesResult = RowSerializer.Serialize(row);
            Assert.That(rowBytesResult.IsSuccess, Is.True);
            var rowBytes = rowBytesResult.Value;
            var env = new ContextEnvironment { Columns = columns, Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(env);

            _mockIndexManager.Setup(im => im.GetContextData(TestContextName, hashBytes)).Returns(Result<byte[]>.Success(rowBytes));

            // Act
            var result = await _contextManager.GetContextInstance(TestContextName, instanceHash);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.Values[0], Is.EqualTo(123));
        }

        [Test]
        public async Task GetContextInstance_GetDataFails_ReturnsError()
        {
            // Arrange
            var instanceHash = "hash1";
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);
            var error = new Error(ErrorPrefixes.FileError, "Not found");

            _mockIndexManager.Setup(im => im.GetContextData(TestContextName, hashBytes)).Returns(Result<byte[]>.Failure(error));

            // Act
            var result = await _contextManager.GetContextInstance(TestContextName, instanceHash);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error, Is.EqualTo(error));
        }

        [Test]
        public async Task GetContextInstance_GetEnvironmentFails_ReturnsError()
        {
            // Arrange
            var instanceHash = "hash1";
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);
            var rowBytes = new byte[] { 1, 2, 3 };

            _mockIndexManager.Setup(im => im.GetContextData(TestContextName, hashBytes)).Returns(Result<byte[]>.Success(rowBytes));

            // Act
            var result = await _contextManager.GetContextInstance(TestContextName, instanceHash);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Is.EqualTo("Context Environment does not exist"));
        }

        [Test]
        public async Task GetContextInstance_DeserializationFails_ReturnsError()
        {
            // Arrange
            var instanceHash = "hash1";
            var hashBytes = IndexKeyConverter.ToByteArray(instanceHash);
            var corruptedRowBytes = new byte[] { 1, 2 }; // Corrupted data
            var columns = new[] { Column.Create("Id", DataType.INT).Value };
            var env = new ContextEnvironment { Columns = columns, Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(env);

            _mockIndexManager.Setup(im => im.GetContextData(TestContextName, hashBytes)).Returns(Result<byte[]>.Success(corruptedRowBytes));

            // Act
            var result = await _contextManager.GetContextInstance(TestContextName, instanceHash);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Prefix, Is.EqualTo(ErrorPrefixes.DataError).Or.EqualTo(ErrorPrefixes.CriticalError));
        }

        [Test]
        public async Task GetAllContextInstances_HappyPath_ReturnsAllRows()
        {
            // Arrange
            var instanceHash1 = "hash1";
            var instanceHash2 = "hash2";
            var columns = new[] { Column.Create("Id", DataType.INT).Value };
            var row1 = Row.Create(new object[] { 1 }, columns).Value;
            var row2 = Row.Create(new object[] { 2 }, columns).Value;
            var rowBytes1 = RowSerializer.Serialize(row1).Value;
            var rowBytes2 = RowSerializer.Serialize(row2).Value;
            var env = new ContextEnvironment { Columns = columns, Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(env);

            var allContextData = new Dictionary<byte[], byte[]>
            {
                { IndexKeyConverter.ToByteArray(instanceHash1), rowBytes1 },
                { IndexKeyConverter.ToByteArray(instanceHash2), rowBytes2 }
            };

            _mockIndexManager.Setup(im => im.GetAllContextData(TestContextName)).Returns(Result<IEnumerable<KeyValuePair<byte[], byte[]>>>.Success(allContextData.AsEnumerable()));

            // Act
            var result = await _contextManager.GetAllContextInstances(TestContextName);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.Count(), Is.EqualTo(2));
            Assert.That(result.Value.First().Values[0], Is.EqualTo(1));
            Assert.That(result.Value.Last().Values[0], Is.EqualTo(2));
        }

        [Test]
        public async Task GetAllContextInstances_GetAllContextDataFails_ReturnsError()
        {
            // Arrange
            var error = new Error(ErrorPrefixes.FileError, "Failed to get all data");
            _mockIndexManager.Setup(im => im.GetAllContextData(TestContextName)).Returns(Result<IEnumerable<KeyValuePair<byte[], byte[]>>>.Failure(error));

            // Act
            var result = await _contextManager.GetAllContextInstances(TestContextName);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error, Is.EqualTo(error));
        }

        [Test]
        public async Task GetAllContextInstances_GetEnvironmentFails_ReturnsError()
        {
            // Arrange
            var instanceHash1 = "hash1";
            var row1 = Row.Create(new object[] { 1 }, new[] { Column.Create("Id", DataType.INT).Value }).Value;
            var rowBytes1 = RowSerializer.Serialize(row1).Value;

            var allContextData = new Dictionary<byte[], byte[]>
            {
                { IndexKeyConverter.ToByteArray(instanceHash1), rowBytes1 }
            };

            _mockIndexManager.Setup(im => im.GetAllContextData(TestContextName)).Returns(Result<IEnumerable<KeyValuePair<byte[], byte[]>>>.Success(allContextData.AsEnumerable()));

            // Act
            // Do NOT create the fake environment file, so GetEnvironment will fail
            var result = await _contextManager.GetAllContextInstances(TestContextName);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Is.EqualTo("Context Environment does not exist"));
        }

        [Test]
        public async Task GetAllContextInstances_DeserializationFailsForSomeRows_ReturnsOnlySuccessfulRows()
        {
            // Arrange
            var instanceHash1 = "hash1";
            var instanceHash2 = "hash2";
            var columns = new[] { Column.Create("Id", DataType.INT).Value };
            var row1 = Row.Create(new object[] { 1 }, columns).Value;
            var rowBytes1 = RowSerializer.Serialize(row1).Value;
            var corruptedRowBytes = new byte[] { 1, 2 }; // Corrupted data

            var env = new ContextEnvironment { Columns = columns, Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(env);

            var allContextData = new Dictionary<byte[], byte[]>
            {
                { IndexKeyConverter.ToByteArray(instanceHash1), rowBytes1 },
                { IndexKeyConverter.ToByteArray(instanceHash2), corruptedRowBytes }
            };

            _mockIndexManager.Setup(im => im.GetAllContextData(TestContextName)).Returns(Result<IEnumerable<KeyValuePair<byte[], byte[]>>>.Success(allContextData.AsEnumerable()));

            // Act
            var result = await _contextManager.GetAllContextInstances(TestContextName);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value.Count(), Is.EqualTo(1)); // Only one successful row
            Assert.That(result.Value.First().Values[0], Is.EqualTo(1));
        }

        [Test]
        public async Task CreateTable_GetEnvironmentFails_ReturnsError()
        {
            // Arrange
            var newTableResult = Table.Create("NewTable", new[] { Column.Create("Col1", DataType.VARCHAR).Value });
            Assert.That(newTableResult.IsSuccess, Is.True);
            
            // Act
            // Do NOT create fake context environment file, so GetEnvironment will fail
            var result = await _contextManager.CreateTable("NonExistentContext", newTableResult.Value);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Is.EqualTo("Context Environment does not exist"));
        }

        [Test]
        public async Task CreateTable_DatabaseNotSet_ReturnsError()
        {
            // Arrange
            _mockSessionState.Setup(ss => ss.CurrentDatabase).Returns((Database)null);
            var newTableResult = Table.Create("NewTable", new[] { Column.Create("Col1", DataType.VARCHAR).Value });
            Assert.That(newTableResult.IsSuccess, Is.True);
            var initialEnv = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(initialEnv);

            // Act
            var result = await _contextManager.CreateTable(TestContextName, newTableResult.Value);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error, Is.InstanceOf<DatabaseNotSetError>());
        }

        [Test]
        public async Task CreateTable_DirectoryCreationFails_ReturnsError()
        {
            // Arrange
            var tableName = "NewTable";
            var newTableResult = Table.Create(tableName, new[] { Column.Create("Col1", DataType.VARCHAR).Value });
            Assert.That(newTableResult.IsSuccess, Is.True);
            var initialEnv = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = Array.Empty<Table>() };
            CreateFakeContextEnvironmentFile(initialEnv);

            _mockFileSystem.Setup(fs => fs.Directory.CreateDirectory(It.IsAny<string>())).Throws<IOException>();

            // Act
            var result = await _contextManager.CreateTable(TestContextName, newTableResult.Value);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Prefix, Is.EqualTo(ErrorPrefixes.FileError));
        }

        [Test]
        public async Task DeleteTable_DatabaseNotSet_ReturnsError()
        {
            // Arrange
            _mockSessionState.Setup(ss => ss.CurrentDatabase).Returns((Database)null);
            var tableName = "TableToDelete";
            var tableResult = Table.Create(tableName, Array.Empty<Column>());
            Assert.That(tableResult.IsSuccess, Is.True);
            var table = tableResult.Value;
            var initialEnv = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = new[] { table } };
            CreateFakeContextEnvironmentFile(initialEnv);

            // Act
            var result = await _contextManager.DeleteTable(TestContextName, tableName);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error, Is.InstanceOf<DatabaseNotSetError>());
        }

        [Test]
        public async Task DeleteTable_GetEnvironmentFails_ReturnsError()
        {
            // Arrange
            var tableName = "TableToDelete";
            // Do NOT create fake context environment file, so GetEnvironment will fail

            // Act
            var result = await _contextManager.DeleteTable("NonExistentContext", tableName);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Is.EqualTo("Context Environment does not exist"));
        }

        [Test]
        public async Task DeleteTable_RenameFolderFails_ReturnsError()
        {
            // Arrange
            var tableName = "TableToDelete";
            var tableResult = Table.Create(tableName, Array.Empty<Column>());
            Assert.That(tableResult.IsSuccess, Is.True);
            var table = tableResult.Value;
            var initialEnv = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = new[] { table } };
            CreateFakeContextEnvironmentFile(initialEnv);

            string tablePath = _concreteFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, tableName);
            string trashPath = _concreteFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, "trash_" + tableName);
            _mockFolderManager.Setup(fm => fm.RenameFolder(tablePath, trashPath)).Throws<IOException>();

            // Act
            var result = await _contextManager.DeleteTable(TestContextName, tableName);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Prefix, Is.EqualTo(ErrorPrefixes.FileError));
            Assert.That(result.Error.Message, Is.EqualTo("Could not prepare table for deletion (Folder Rename Failed)."));
        }

        // [Test]
        // public async Task DeleteTable_SaveEnvironmentFails_RollsBack_ReturnsError()
        // {
        //     // Arrange
        //     var tableName = "TableToDelete";
        //     var tableResult = Table.Create(tableName, Array.Empty<Column>());
        //     Assert.That(tableResult.IsSuccess, Is.True);
        //     var table = tableResult.Value;
        //     var initialEnv = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = new[] { table } };
            
        //     // Explicitly set up GetEnvironment to succeed
        //     var serializedEnv = ByteObjectConverter.ObjectToByteArray(initialEnv);
            
        //     // Ensure the file path for the environment is correct
        //     var expectedEnvFilePath = _mockFileSystem.Object.Path.Combine(_basePath, TestDbName, TestContextName, "_env");

        //     _mockFileSystem.Setup(fs => fs.File.Exists(expectedEnvFilePath)).Returns(true);
        //     _mockFileSystem.Setup(fs => fs.File.ReadAllBytes(expectedEnvFilePath)).Returns(serializedEnv);

        //     string tablePath = _concreteFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, tableName);
        //     string trashPath = _concreteFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, "trash_" + tableName);

        //     // Mock SaveEnvironment to fail by making WriteBytesToFile (used by SaveEnvironment) throw an exception
        //     _mockFileSystem.Setup(fs => fs.File.WriteAllBytes(It.IsAny<string>(), It.IsAny<byte[]>())).Throws<IOException>();

        //     // Act
        //     var result = await _contextManager.DeleteTable(TestContextName, tableName);

        //     // Assert
        //     Assert.That(result.IsFailure, Is.True);
        //     Assert.That(result.Error.Message, Is.EqualTo("Failed to update context definition. Deletion reverted."));
        //     // Verify that rename folder was called twice (once for initial rename, once for rollback)
        //     _mockFolderManager.Verify(fm => fm.RenameFolder(It.IsAny<string>(), It.IsAny<string>()), Times.Exactly(2));
        // }
        
        // [Test]
        // public async Task DeleteTable_DeleteFolderFails_ReturnsSuccess()
        // {
        //     // Arrange
        //     var tableName = "TableToDelete";
        //     var tableResult = Table.Create(tableName, Array.Empty<Column>());
        //     Assert.That(tableResult.IsSuccess, Is.True);
        //     var table = tableResult.Value;
        //     var initialEnv = new ContextEnvironment { Columns = Array.Empty<Column>(), Tables = new[] { table } };
            
        //     // Explicitly set up GetEnvironment to succeed
        //     var serializedEnv = ByteObjectConverter.ObjectToByteArray(initialEnv);
        //     var expectedEnvFilePath = _mockFileSystem.Object.Path.Combine(_basePath, TestDbName, TestContextName, "_env");
        //     _mockFileSystem.Setup(fs => fs.File.Exists(expectedEnvFilePath)).Returns(true);
        //     _mockFileSystem.Setup(fs => fs.File.ReadAllBytes(expectedEnvFilePath)).Returns(serializedEnv);

        //     string tablePath = _concreteFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, tableName);
        //     string trashPath = _concreteFileSystem.Path.Combine(_basePath, TestDbName, TestContextName, "trash_" + tableName);
            
        //     // Mock DeleteFolder to throw an exception
        //     _mockFolderManager.Setup(fm => fm.DeleteFolder(trashPath)).Throws<IOException>();

        //     // Act
        //     var result = await _contextManager.DeleteTable(TestContextName, tableName);

        //     // Assert
        //     Assert.That(result.IsSuccess, Is.True);
        //     _mockFolderManager.Verify(fm => fm.RenameFolder(tablePath, trashPath), Times.Once);
        //     _mockFolderManager.Verify(fm => fm.DeleteFolder(trashPath), Times.Once); // Still verify it was called
        // }
    }
}
