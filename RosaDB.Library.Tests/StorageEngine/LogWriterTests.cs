#nullable disable

using System.IO.Abstractions.TestingHelpers;
using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using RosaDB.Library.Websockets.Interfaces;

namespace RosaDB.Library.Tests.StorageEngine
{
    [TestFixture]
    public class LogWriterTests
    {
        private Mock<ILogCondenser> _mockLogCondenser;
        private Mock<SessionState> _mockSessionState;
        private MockFileSystem _mockFileSystem;
        private Mock<IFolderManager> _mockFolderManager;
        private Mock<IIndexManager> _mockIndexManager;
        private Mock<IContextManager> _mockContextManager;
        private Mock<ISubscriptionManager> _mockSubscriptionManager;
        private WriteAheadLogCache _writeAheadLogCache;
        private LogWriter _logWriter;

        private const string contextName = "TestContext";
        private const string tableName = "TestTable";

        private readonly Column[] tableColumns =
        [
            Column.Create("LogId", DataType.BIGINT, isPrimaryKey: true).Value,
            Column.Create("data", DataType.VARCHAR).Value
        ];

        private byte[] fakeData1;
        
        [SetUp]
        public void Setup()
        {
            _mockLogCondenser = new Mock<ILogCondenser>();
            _mockSessionState = new Mock<SessionState>();
            _mockFileSystem = new MockFileSystem();
            _mockFolderManager = new Mock<IFolderManager>();
            _mockIndexManager = new Mock<IIndexManager>();
            _mockContextManager = new Mock<IContextManager>();
            _mockSubscriptionManager = new Mock<ISubscriptionManager>();
            _writeAheadLogCache = new WriteAheadLogCache();

            var mockDatabase = Database.Create("TestDb").Value;
            _mockSessionState.Setup(s => s.CurrentDatabase).Returns(mockDatabase);
            _mockFolderManager.Setup(f => f.BasePath).Returns(@"C:\Test");

            _mockContextManager.Setup(cm => cm.GetColumnsFromTable(It.IsAny<string>(), It.IsAny<string>())).ReturnsAsync(tableColumns);
            
            fakeData1 = RowSerializer.Serialize(Row.Create([(long)1, "data1"], tableColumns).Value).Value;
            
            _logWriter = new LogWriter(
                _mockLogCondenser.Object, 
                _mockSessionState.Object, 
                _mockFileSystem, 
                _mockFolderManager.Object,
                _mockIndexManager.Object,
                _mockContextManager.Object,
                _mockSubscriptionManager.Object,
                _writeAheadLogCache);
        }

        [Test]
        public async Task Commit_WritesLogsToDisk()
        {
            var contextIndex = new object[] { 1 };
            var logId = 12345;
            var log = new Log { Id = logId, TupleData = fakeData1 };

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>())).Returns([log]);

            _logWriter.Put(contextName, tableName, contextIndex, fakeData1, logId: logId);

            var identifier = InstanceHasher.CreateIdentifier(contextName, tableName, contextIndex);

            _mockIndexManager.Setup(im => im.Insert(
                It.Is<TableInstanceIdentifier>(i => i.Equals(identifier)),
                It.Is<string>(s => s == "LogId"),
                It.Is<byte[]>(b => b.SequenceEqual(IndexKeyConverter.ToByteArray(logId))),
                It.IsAny<LogLocation>()));

            // Act
            var result = await _logWriter.Commit();

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            var expectedPath = GetExpectedSegmentFilePath(identifier, 0);
            Assert.That(_mockFileSystem.File.Exists(expectedPath), Is.True);
        }

        [Test]
        public async Task Commit_ReturnsError_WhenDatabaseNotSet()
        {
            _mockSessionState.Setup(s => s.CurrentDatabase).Returns((Database)null);

            // Act
            var result = await _logWriter.Commit();

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error!.Message, Is.EqualTo("Database is not set"));
        }

        [Test]
        public void Put_AddsLogToWriteAheadLog()
        {
            var contextIndex = new object[] { 1 };
            var logId = 12345;

            // Act
            _logWriter.Put(contextName, tableName, contextIndex, fakeData1, logId: logId);
            var identifier = InstanceHasher.CreateIdentifier(contextName, tableName, contextIndex);

            // Assert
            Assert.That(_writeAheadLogCache.Logs.ContainsKey(identifier), Is.True);
            Assert.That(_writeAheadLogCache.Logs[identifier].Count, Is.EqualTo(1));
            var log = _writeAheadLogCache.Logs[identifier].Peek();
            Assert.That(log.Id, Is.EqualTo(logId));
            Assert.That(log.TupleData, Is.EqualTo(fakeData1));
            Assert.That(log.IsDeleted, Is.False);
        }

        [Test]
        public void Delete_AddsTombstoneLogToWriteAheadLog()
        {
            var contextIndex = new object[] { 1 };
            var logId = 54321;

            // Act
            _logWriter.Delete(contextName, tableName, contextIndex, logId);
            var identifier = InstanceHasher.CreateIdentifier(contextName, tableName, contextIndex);

            // Assert
            Assert.That(_writeAheadLogCache.Logs.ContainsKey(identifier), Is.True);
            Assert.That(_writeAheadLogCache.Logs[identifier].Count, Is.EqualTo(1));
            var log = _writeAheadLogCache.Logs[identifier].Peek();
            Assert.That(log.Id, Is.EqualTo(logId));
            Assert.That(log.IsDeleted, Is.True);
        }

        [TearDown]
        public async Task TearDown()
        {
            if (_logWriter != null)
                await _logWriter.DisposeAsync();
        }

        private string GetExpectedSegmentFilePath(TableInstanceIdentifier identifier, int segmentNumber)
        {
            var hashPrefix = identifier.InstanceHash.Length >= 2 
                ? identifier.InstanceHash.Substring(0, 2) 
                : "xy"; 

            return _mockFileSystem.Path.Combine(
                _mockFolderManager.Object.BasePath, _mockSessionState.Object.CurrentDatabase.Name, 
                identifier.ContextName, identifier.TableName, 
                hashPrefix,
                $"{identifier.InstanceHash}_{segmentNumber}.dat");
        }
    }
}
