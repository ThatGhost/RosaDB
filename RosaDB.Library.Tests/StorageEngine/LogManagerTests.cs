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
using RosaDB.Library.MoqQueries;
using RosaDB.Library.Websockets.Interfaces;

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
        private Mock<IContextManager> _mockContextManager;
        private Mock<ISubscriptionManager> _mockSubscriptionManager;
        private LogManager _logManager;

        private const string contextName = "TestContext";
        private const string tableName = "TestTable";

        private readonly Column[] tableColumns =
        [
            Column.Create("LogId", DataType.BIGINT, isPrimaryKey: true).Value,
            Column.Create("data", DataType.VARCHAR).Value
        ];

        private byte[] fakeData1;
        private byte[] fakeData2;
        
        [SetUp]
        public void Setup()
        {
            _mockLogCondenser = new Mock<LogCondenser>();
            _mockSessionState = new Mock<SessionState>();
            _mockFileSystem = new MockFileSystem();
            _mockFolderManager = new Mock<IFolderManager>();
            _mockIndexManager = new Mock<IIndexManager>();
            _mockContextManager = new Mock<IContextManager>();
            _mockSubscriptionManager = new Mock<ISubscriptionManager>();

            var mockDatabase = Database.Create("TestDb").Value;
            _mockSessionState.Setup(s => s.CurrentDatabase).Returns(mockDatabase);
            _mockFolderManager.Setup(f => f.BasePath).Returns(@"C:\Test");

            _mockContextManager.Setup(cm => cm.GetColumnsFromTable(It.IsAny<string>(), It.IsAny<string>())).ReturnsAsync(tableColumns);
            
            fakeData1 = RowSerializer.Serialize(Row.Create([(long)1, "data1"], tableColumns).Value).Value;
            fakeData2 = RowSerializer.Serialize(Row.Create([(long)2, "data2"], tableColumns).Value).Value;
            
            _logManager = new LogManager(
                _mockLogCondenser.Object, 
                _mockSessionState.Object, 
                _mockFileSystem, 
                _mockFolderManager.Object,
                _mockIndexManager.Object,
                _mockContextManager.Object,
                _mockSubscriptionManager.Object);
        }

        // This goes through a whole flow 
        // -> Init database
        // -> Create database
        // -> use database
        // -> create context
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

            IContextManager realContextManager = new ContextManager(sessionState, fileSystem, folderManager, indexManager);
            IDatabaseManager databaseManager = new DatabaseManager(sessionState, realContextManager, fileSystem, folderManager);
            var rootManager = new RootManager(databaseManager, sessionState, fileSystem, folderManager);
            var mockSubscriptionManager = new Mock<ISubscriptionManager>();
            var logManager = new LogManager(new LogCondenser(), sessionState, fileSystem, folderManager, indexManager, realContextManager, mockSubscriptionManager.Object);
            
            var createDbQuery = new CreateDatabaseQuery(rootManager);
            var useDbQuery = new UseDatabaseQuery(rootManager, sessionState);
            var createContextQuery = new CreateContextQuery(databaseManager);
            var createTableQuery = new CreateTableDefinition(realContextManager);
            var writeQuery = new WriteLogAndCommitQuery(logManager, realContextManager);
            var updateQuery = new UpdateContextLogsQuery(logManager, realContextManager);
            var getQuery = new GetContextLogsQuery(logManager, realContextManager);

            mockIndexManager.Setup(im => im.Insert(
                It.IsAny<TableInstanceIdentifier>(),
                It.IsAny<string>(),
                It.IsAny<byte[]>(),
                It.IsAny<LogLocation>()));
            
            mockIndexManager.Setup(im => im.Search(
                It.IsAny<TableInstanceIdentifier>(),
                It.IsAny<string>(),
                It.IsAny<byte[]>()))
                .Returns(Result<LogLocation>.Success(new LogLocation(0,0)));

            try
            {
                var initializeRootResult = await rootManager.InitializeRoot();
                Assert.That(initializeRootResult.IsSuccess, Is.True, $"InitializeRoot failed: {initializeRootResult.Error?.Message}");

                var createDbResult = await createDbQuery.Execute("db");
                Assert.That(createDbResult.IsSuccess, Is.True, $"CreateDatabaseQuery failed: {createDbResult.Error?.Message}");

                var useDbResult = await useDbQuery.Execute("db");
                Assert.That(useDbResult.IsSuccess, Is.True, $"UseDatabaseQuery failed: {useDbResult.Error?.Message}");
                var createContextResult = await createContextQuery.Execute("context");
                Assert.That(createContextResult.IsSuccess, Is.True, $"CreateContextQuery failed: {createContextResult.Error?.Message}");

                var createTableResult = await createTableQuery.Execute("context", "table");
                Assert.That(createTableResult.IsSuccess, Is.True, $"CreateTableDefinition failed: {createTableResult.Error?.Message}");

                await writeQuery.Execute("context", "table", "initial data");

                Assert.That(_mockFileSystem.Directory.Exists(Path.Combine(tempDirectory, "db", "context", "table")), Is.True, "Table directory should exist in MockFileSystem.");

                var files = _mockFileSystem.Directory.GetFiles(Path.Combine(tempDirectory, "db", "context", "table"), "*.dat", SearchOption.AllDirectories);
                Assert.That(files, Is.Not.Empty, "Data files should be created after write query.");

                var initialResult = await getQuery.Execute("context", "table", [2]);
                Assert.That(initialResult, Is.Not.Empty, "GetContextLogsQuery should return results before update.");

                await updateQuery.Execute("context", "table", [2], "updated data");
                var result = await getQuery.Execute("context", "table", [2]);

                Assert.That(result, Is.Not.Empty, "GetContextLogsQuery should return results after update.");
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
        public async Task GetAllLogsForContextInstanceTable_ReturnsOnlyLogsForInstance()
        {
            var contextIndex1 = new object[] { 1 };
            var logId1 = 12345;

            var contextIndex2 = new object[] { 2 };
            var logId2 = 54321;

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>()))
                .Returns((Queue<Log> q) => q.ToList());

            _logManager.Put(contextName, tableName, contextIndex1, fakeData1, logId: logId1);
            await _logManager.Commit();
            _logManager.Put(contextName, tableName, contextIndex2, fakeData2, logId: logId2);
            await _logManager.Commit();

            // Act
            var allLogs = new List<Log>();
            await foreach (var log in _logManager.GetAllLogsForContextInstanceTable(contextName, tableName, contextIndex1))
            {
                allLogs.Add(log);
            }

            // Assert
            Assert.That(allLogs.Count, Is.EqualTo(1));
            Assert.That(allLogs[0].Id, Is.EqualTo(logId1));
        }

        [Test]
        public async Task GetAllLogsForContextTable_ReturnsAllLogsForTable()
        {
            var contextIndex1 = new object[] { 1 };
            var logId1 = 12345;

            var contextIndex2 = new object[] { 2 };
            var logId2 = 54321;

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>()))
                .Returns((Queue<Log> q) => q.ToList());

            _logManager.Put(contextName, tableName, contextIndex1, fakeData1, logId: logId1);
            await _logManager.Commit();
            _logManager.Put(contextName, tableName, contextIndex2, fakeData2, logId: logId2);
            await _logManager.Commit();

            // Act
            var allLogs = new List<Log>();
            await foreach (var log in _logManager.GetAllLogsForContextTable(contextName, tableName))
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
            var contextIndex = new object[] { 1 };
            long logId = 12345;
            var log = new Log { Id = logId, TupleData = fakeData1 };

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>())).Returns([log]);

            // Set up the expected file path and content
            var identifier = CreateIdentifier(contextIndex);
            var segmentPath = GetExpectedSegmentFilePath(contextIndex, 0); 
            _mockFileSystem.AddFile(segmentPath, new MockFileData(LogSerializer.Serialize(log)));

            // Mock IndexManager.Search to return the location of the log
            _mockIndexManager.Setup(im => im.Search(
                It.Is<TableInstanceIdentifier>(i => i.InstanceHash.Equals(identifier.InstanceHash)),
                It.Is<string>(s => s == "LogId"),
                It.Is<byte[]>(b => b.SequenceEqual(IndexKeyConverter.ToByteArray(logId)))))
                .Returns(new LogLocation(0, 0));

            _logManager.Put(contextName, tableName, contextIndex, fakeData1, logId: logId);
            await _logManager.Commit();

            // Act
            var result = await _logManager.FindLastestLog(contextName, tableName, contextIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.Id, Is.EqualTo(logId));
            Assert.That(result.Value.TupleData, Is.EqualTo(fakeData1));
            Assert.That(result.Value.IsDeleted, Is.False);
        }

        [Test]
        public async Task Commit_WritesLogsToDisk()
        {
            var contextIndex = new object[] { 1 };
            var logId = 12345;
            var log = new Log { Id = logId, TupleData = fakeData1 };

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>())).Returns([log]);

            _logManager.Put(contextName, tableName, contextIndex, fakeData1, logId: logId);

            var identifier = CreateIdentifier(contextIndex);

            _mockIndexManager.Setup(im => im.Insert(
                It.Is<TableInstanceIdentifier>(i => i.Equals(identifier)),
                It.Is<string>(s => s == "LogId"),
                It.Is<byte[]>(b => b.SequenceEqual(IndexKeyConverter.ToByteArray(logId))),
                It.IsAny<LogLocation>()));

            // Act
            var result = await _logManager.Commit();

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var expectedPath = GetExpectedSegmentFilePath(contextIndex, 0);
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
            Assert.That(result.Error!.Message, Is.EqualTo("Database is not set"));
        }

        private string GetExpectedSegmentFilePath(object[] indexValues, int segmentNumber)
        {
            var indexString = string.Join(";", indexValues.Select(v => v.ToString()));
            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(indexString)));
            var hashPrefix = hash.Substring(0, 2);
            return _mockFileSystem.Path.Combine(
                _mockFolderManager.Object.BasePath, 
                _mockSessionState.Object.CurrentDatabase!.Name, 
                contextName, 
                tableName, 
                hashPrefix,
                $"{hash}_{segmentNumber}.dat");
        }

        private TableInstanceIdentifier CreateIdentifier(object[] indexValues)
        {
            var indexString = string.Join(";", indexValues.Select(v => v.ToString()));
            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(indexString)));
            return new TableInstanceIdentifier(contextName, tableName, hash);
        }

        [Test]
        public async Task Put_AddsLogToWriteAheadLog()
        {
            var contextIndex = new object[] { 1 };
            var logId = 12345;

            // Act
            _logManager.Put(contextName, tableName, contextIndex, fakeData1, logId: logId);
            var result = await _logManager.FindLastestLog(contextName, tableName, contextIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.Id, Is.EqualTo(logId));
            Assert.That(result.Value.TupleData, Is.EqualTo(fakeData1));
            Assert.That(result.Value.IsDeleted, Is.False);
        }

        [Test]
        public async Task Delete_AddsTombstoneLogToWriteAheadLog()
        {
            var contextIndex = new object[] { 1 };
            var logId = 54321;

            // Act
            _logManager.Delete(contextName, tableName, contextIndex, logId);
            var result = await _logManager.FindLastestLog(contextName, tableName, contextIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.Id, Is.EqualTo(logId));
            Assert.That(result.Value.IsDeleted, Is.True);
        }

        [Test]
        public async Task FindLastestLog_ReturnsLatestLog_WhenMultipleUpdates()
        {
            var contextIndex = new object[] { 1 };
            var logId = 67890;

            // Act
            _logManager.Put(contextName, tableName, contextIndex, fakeData1, logId: logId);
            _logManager.Put(contextName, tableName, contextIndex, fakeData2, logId: logId);
            var result = await _logManager.FindLastestLog(contextName, tableName, contextIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.TupleData, Is.EqualTo(fakeData2));
        }

        [TearDown]
        public async Task TearDown()
        {
            if (_logManager != null)
                await _logManager.DisposeAsync();
        }
    }
}