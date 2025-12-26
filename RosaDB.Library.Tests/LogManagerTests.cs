#nullable disable

using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions.TestingHelpers;
using System.Security.Cryptography;
using System.Text;
using System.IO.Abstractions;
using RosaDB.Library.MoqQueries;

namespace RosaDB.Library.Tests
{
    [TestFixture]
    public class LogManagerTests
    {
        private Mock<LogCondenser> _mockLogCondenser;
        private Mock<SessionState> _mockSessionState;
        private MockFileSystem _mockFileSystem;
        private Mock<IFolderManager> _mockFolderManager;
        private Mock<IIndexManager> _mockIndexManager;
        private LogManager _logManager;

        private string cellName = "TestCell";
        private string tableName = "TestTable";

        [SetUp]
        public void Setup()
        {
            _mockLogCondenser = new Mock<LogCondenser>();
            _mockSessionState = new Mock<SessionState>();
            _mockFileSystem = new MockFileSystem();
            _mockFolderManager = new Mock<IFolderManager>();
            _mockIndexManager = new Mock<IIndexManager>();

            var mockDatabase = Database.Create("TestDb").Value;
            _mockSessionState.Setup(s => s.CurrentDatabase).Returns(mockDatabase);
            _mockFolderManager.Setup(f => f.BasePath).Returns(@"C:\Test");

            _logManager = new LogManager(
                _mockLogCondenser.Object, 
                _mockSessionState.Object, 
                _mockFileSystem, 
                _mockFolderManager.Object,
                _mockIndexManager.Object);
        }

        // This goes through a whole flow 
        // -> Init database
        // -> Create database
        // -> use database
        // -> create cell
        // -> create table
        // -> add logs
        // -> update logs
        // -> get logs
        [Test]
        public async Task Full_IntegrationTestFlow()
        {
            var tempDirectory = Path.Combine(Path.GetTempPath(), "rosadb_test_" + Path.GetRandomFileName());
            
            var fileSystem = _mockFileSystem; 
            var folderManager = new FolderManager(fileSystem) { BasePath = tempDirectory };
            var sessionState = new SessionState();
            
            Mock<IIndexManager> mockIndexManager = new Mock<IIndexManager>();
            IIndexManager indexManager = mockIndexManager.Object;

            ICellManager cellManager = new CellManager(sessionState, fileSystem, folderManager);
            IDatabaseManager databaseManager = new DatabaseManager(sessionState, cellManager, fileSystem, folderManager);
            var rootManager = new RootManager(databaseManager, sessionState, fileSystem, folderManager);
            var logManager = new LogManager(new LogCondenser(), sessionState, fileSystem, folderManager, indexManager);
            
            var createDbQuery = new CreateDatabaseQuery(rootManager);
            var useDbQuery = new UseDatabaseQuery(rootManager, sessionState);
            var createCellQuery = new CreateCellQuery(databaseManager);
            var createTableQuery = new CreateTableDefinition(cellManager);
            var writeQuery = new WriteLogAndCommitQuery(logManager, cellManager);
            var updateQuery = new UpdateCellLogsQuery(logManager, cellManager);
            var getQuery = new GetCellLogsQuery(logManager, cellManager);

            mockIndexManager.Setup(im => im.Insert(
                It.IsAny<TableInstanceIdentifier>(),
                It.IsAny<string>(),
                It.IsAny<long>(),
                It.IsAny<LogLocation>()));
            
            mockIndexManager.Setup(im => im.Search(
                It.IsAny<TableInstanceIdentifier>(),
                It.IsAny<string>(),
                It.IsAny<long>()))
                .Returns(new Error(ErrorPrefixes.DataError, "Log not found in mocked index."));

            try
            {
                var initializeRootResult = await rootManager.InitializeRoot();
                Assert.That(initializeRootResult.IsSuccess, Is.True, $"InitializeRoot failed: {initializeRootResult.Error?.Message}");

                var createDbResult = await createDbQuery.Execute("db");
                Assert.That(createDbResult.IsSuccess, Is.True, $"CreateDatabaseQuery failed: {createDbResult.Error?.Message}");

                var useDbResult = await useDbQuery.Execute("db");
                Assert.That(useDbResult.IsSuccess, Is.True, $"UseDatabaseQuery failed: {useDbResult.Error?.Message}");
                var createCellResult = await createCellQuery.Execute("cell");
                Assert.That(createCellResult.IsSuccess, Is.True, $"CreateCellQuery failed: {createCellResult.Error?.Message}");

                var createTableResult = await createTableQuery.Execute("cell", "table");
                Assert.That(createTableResult.IsSuccess, Is.True, $"CreateTableDefinition failed: {createTableResult.Error?.Message}");

                await writeQuery.Execute("cell", "table", "initial data");

                Assert.That(_mockFileSystem.Directory.Exists(Path.Combine(tempDirectory, "db", "cell", "table")), Is.True, "Table directory should exist in MockFileSystem.");

                var files = _mockFileSystem.Directory.GetFiles(Path.Combine(tempDirectory, "db", "cell", "table"), "*.dat", SearchOption.AllDirectories);
                Assert.That(files, Is.Not.Empty, "Data files should be created after write query.");

                var initialResult = await getQuery.Execute("cell", "table", new object[] { 2 });
                Assert.That(initialResult, Is.Not.Empty, "GetCellLogsQuery should return results before update.");

                await updateQuery.Execute("cell", "table", new object[] { 2 }, "updated data");
                var result = await getQuery.Execute("cell", "table", new object[] { 2 });

                Assert.That(result, Is.Not.Empty, "GetCellLogsQuery should return results after update.");
                Assert.That(result.All(r => r.Contains("updated data")), Is.True, "All results should contain 'updated data'.");
            }
            finally
            {
                if (_mockFileSystem.Directory.Exists(tempDirectory))
                {
                    _mockFileSystem.Directory.Delete(tempDirectory, true);
                }
            }
        }

        [Test]
        public async Task GetAllLogsForCellInstanceTable_ReturnsOnlyLogsForInstance()
        {
            var tableIndex1 = new object[] { 1 };
            var logId1 = 12345;
            var data1 = new byte[] { 1, 1, 1 };

            var tableIndex2 = new object[] { 2 };
            var logId2 = 54321;
            var data2 = new byte[] { 2, 2, 2 };

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>()))
                .Returns((Queue<Log> q) => q.ToList());

            _logManager.Put(cellName, tableName, tableIndex1, data1, logId1);
            await _logManager.Commit();
            _logManager.Put(cellName, tableName, tableIndex2, data2, logId2);
            await _logManager.Commit();

            // Act
            var allLogs = new List<Log>();
            await foreach (var log in _logManager.GetAllLogsForCellInstanceTable(cellName, tableName, tableIndex1))
            {
                allLogs.Add(log);
            }

            // Assert
            Assert.That(allLogs.Count, Is.EqualTo(1));
            Assert.That(allLogs[0].Id, Is.EqualTo(logId1));
        }

        [Test]
        public async Task GetAllLogsForCellTable_ReturnsAllLogsForTable()
        {
            var tableIndex1 = new object[] { 1 };
            var logId1 = 12345;
            var data1 = new byte[] { 1, 1, 1 };

            var tableIndex2 = new object[] { 2 };
            var logId2 = 54321;
            var data2 = new byte[] { 2, 2, 2 };

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>()))
                .Returns((Queue<Log> q) => q.ToList());

            _logManager.Put(cellName, tableName, tableIndex1, data1, logId1);
            await _logManager.Commit();
            _logManager.Put(cellName, tableName, tableIndex2, data2, logId2);
            await _logManager.Commit();

            // Act
            var allLogs = new List<Log>();
            await foreach (var log in _logManager.GetAllLogsForCellTable(cellName, tableName))
            {
                allLogs.Add(log);
            }

            // Assert
            Assert.That(allLogs.Count, Is.EqualTo(2));
            Assert.That(allLogs.Any(l => l.Id == logId1), Is.True);
            Assert.That(allLogs.Any(l => l.Id == logId2), Is.True);
        }

        [Test]
        public async Task FindLastestLog_FindsLogOnDisk()
        {
            var tableIndex = new object[] { 1 };
            var logId = 12345;
            var data = new byte[] { 1, 2, 3 };
            var log = new Log { Id = logId, TupleData = data };

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>())).Returns(new List<Log> { log });

            // Setup the expected file path and content
            var identifier = CreateIdentifier(cellName, tableName, tableIndex);
            var segmentPath = GetExpectedSegmentFilePath(tableIndex, 0); 
            _mockFileSystem.AddFile(segmentPath, new MockFileData(LogSerializer.Serialize(log)));

            // Mock IndexManager.Search to return the location of the log
            _mockIndexManager.Setup(im => im.Search(
                It.Is<TableInstanceIdentifier>(i => i.Equals(identifier)),
                It.Is<string>(s => s == "LogId"),
                It.Is<long>(l => l == logId)))
                .Returns(new LogLocation(0, 0)); // Assuming segment 0, offset 0 for simplicity

            _logManager.Put(cellName, tableName, tableIndex, data, logId);
            await _logManager.Commit();

            // Act
            var result = await _logManager.FindLastestLog(cellName, tableName, tableIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.Id, Is.EqualTo(logId));
            Assert.That(result.Value.TupleData, Is.EqualTo(data));
            Assert.That(result.Value.IsDeleted, Is.False);
        }

        [Test]
        public async Task Commit_WritesLogsToDisk()
        {
            var tableIndex = new object[] { 1 };
            var logId = 12345;
            var data = new byte[] { 1, 2, 3 };
            var log = new Log { Id = logId, TupleData = data };

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>())).Returns(new List<Log> { log });

            _logManager.Put(cellName, tableName, tableIndex, data, logId);

            var identifier = CreateIdentifier(cellName, tableName, tableIndex);

            _mockIndexManager.Setup(im => im.Insert(
                It.Is<TableInstanceIdentifier>(i => i.Equals(identifier)),
                It.Is<string>(s => s == "LogId"),
                It.Is<long>(l => l == logId),
                It.IsAny<LogLocation>()));

            // Act
            var result = await _logManager.Commit();

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var expectedPath = GetExpectedSegmentFilePath(tableIndex, 0);
            Assert.That(_mockFileSystem.File.Exists(expectedPath), Is.True);
        }

        [Test]
        public async Task Commit_ReturnsError_WhenDatabaseNotSet()
        {
            _mockSessionState.Setup(s => s.CurrentDatabase).Returns((Database)null);

            // Act
            var result = await _logManager.Commit();

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Is.EqualTo("Database is not set"));
        }

        private string GetExpectedSegmentFilePath(object[] indexValues, int segmentNumber)
        {
            var indexString = string.Join(";", indexValues.Select(v => v.ToString()));
            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(indexString)));
            var hashPrefix = hash.Substring(0, 2);
            return _mockFileSystem.Path.Combine(
                _mockFolderManager.Object.BasePath, 
                _mockSessionState.Object.CurrentDatabase.Name, 
                cellName, 
                tableName, 
                hashPrefix,
                $"{hash}_{segmentNumber}.dat");
        }

        private TableInstanceIdentifier CreateIdentifier(string cellName, string tableName, object[] indexValues)
        {
            var indexString = string.Join(";", indexValues.Select(v => v.ToString()));
            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(indexString)));
            return new TableInstanceIdentifier(cellName, tableName, hash);
        }

        [Test]
        public async Task Put_AddsLogToWriteAheadLog()
        {
            var tableIndex = new object[] { 1 };
            var data = new byte[] { 1, 2, 3 };
            var logId = 12345;

            // Act
            _logManager.Put(cellName, tableName, tableIndex, data, logId);
            var result = await _logManager.FindLastestLog(cellName, tableName, tableIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.Id, Is.EqualTo(logId));
            Assert.That(result.Value.TupleData, Is.EqualTo(data));
            Assert.That(result.Value.IsDeleted, Is.False);
        }

        [Test]
        public async Task Delete_AddsTombstoneLogToWriteAheadLog()
        {
            var tableIndex = new object[] { 1 };
            var logId = 54321;

            // Act
            _logManager.Delete(cellName, tableName, tableIndex, logId);
            var result = await _logManager.FindLastestLog(cellName, tableName, tableIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.Id, Is.EqualTo(logId));
            Assert.That(result.Value.IsDeleted, Is.True);
        }

        [Test]
        public async Task FindLastestLog_ReturnsLatestLog_WhenMultipleUpdates()
        {
            var tableIndex = new object[] { 1 };
            var logId = 67890;
            var data1 = new byte[] { 1, 1, 1 };
            var data2 = new byte[] { 2, 2, 2 };

            // Act
            _logManager.Put(cellName, tableName, tableIndex, data1, logId);
            _logManager.Put(cellName, tableName, tableIndex, data2, logId);
            var result = await _logManager.FindLastestLog(cellName, tableName, tableIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.TupleData, Is.EqualTo(data2));
        }
    }
}