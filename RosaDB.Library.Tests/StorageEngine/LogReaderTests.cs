#nullable disable

using System.IO.Abstractions.TestingHelpers;
using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Tests.StorageEngine
{
    [TestFixture]
    public class LogReaderTests
    {
        private Mock<SessionState> _mockSessionState;
        private MockFileSystem _mockFileSystem;
        private Mock<IFolderManager> _mockFolderManager;
        private Mock<IIndexManager> _mockIndexManager;
        private WriteAheadLogCache _writeAheadLogCache;
        private LogReader _logReader;
        private LogWriter _logWriter;

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
            _mockSessionState = new Mock<SessionState>();
            _mockFileSystem = new MockFileSystem();
            _mockFolderManager = new Mock<IFolderManager>();
            _mockIndexManager = new Mock<IIndexManager>();
            _writeAheadLogCache = new WriteAheadLogCache();

            var mockDatabase = Database.Create("TestDb").Value;
            _mockSessionState.Setup(s => s.CurrentDatabase).Returns(mockDatabase);
            _mockFolderManager.Setup(f => f.BasePath).Returns(@"C:\Test");
            
            fakeData1 = RowSerializer.Serialize(Row.Create([(long)1, "data1"], tableColumns).Value).Value;
            fakeData2 = RowSerializer.Serialize(Row.Create([(long)2, "data2"], tableColumns).Value).Value;
            
            _logReader = new LogReader(
                _mockSessionState.Object, 
                _mockFileSystem, 
                _mockFolderManager.Object,
                _mockIndexManager.Object,
                _writeAheadLogCache);
            
            // We need a writer to put stuff on disk for the reader to read
            var mockLogCondenser = new Mock<ILogCondenser>();
            mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>()))
                .Returns((Queue<Log> q) => q.ToList());

            var mockContextManager = new Mock<IContextManager>();
            mockContextManager.Setup(cm => cm.GetColumnsFromTable(It.IsAny<string>(), It.IsAny<string>())).ReturnsAsync(tableColumns);

            _logWriter = new LogWriter(
                mockLogCondenser.Object,
                _mockSessionState.Object,
                _mockFileSystem,
                _mockFolderManager.Object,
                _mockIndexManager.Object,
                mockContextManager.Object,
                new Mock<Websockets.Interfaces.ISubscriptionManager>().Object,
                _writeAheadLogCache);
        }

        [Test]
        public async Task GetAllLogsForContextInstanceTable_ReturnsOnlyLogsForInstance()
        {
            var contextIndex1 = new object[] { 1 };
            var logId1 = 12345;

            var contextIndex2 = new object[] { 2 };
            var logId2 = 54321;

            _logWriter.Put(contextName, tableName, contextIndex1, fakeData1, logId: logId1);
            await _logWriter.Commit();
            _logWriter.Put(contextName, tableName, contextIndex2, fakeData2, logId: logId2);
            await _logWriter.Commit();

            // Act
            var allLogs = new List<Log>();
            await foreach (var log in _logReader.GetAllLogsForContextInstanceTable(contextName, tableName, contextIndex1))
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

            _logWriter.Put(contextName, tableName, contextIndex1, fakeData1, logId: logId1);
            await _logWriter.Commit();
            _logWriter.Put(contextName, tableName, contextIndex2, fakeData2, logId: logId2);
            await _logWriter.Commit();

            // Act
            var allLogs = new List<Log>();
            await foreach (var log in _logReader.GetAllLogsForContextTable(contextName, tableName))
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
            
            var identifier = InstanceHasher.CreateIdentifier(contextName, tableName, contextIndex);
            var segmentPath = GetExpectedSegmentFilePath(identifier, 0); 
            _mockFileSystem.AddFile(segmentPath, new MockFileData(LogSerializer.Serialize(log)));

            _mockIndexManager.Setup(im => im.Search(
                It.Is<TableInstanceIdentifier>(i => i.InstanceHash.Equals(identifier.InstanceHash)),
                It.Is<string>(s => s == "LogId"),
                It.Is<byte[]>(b => b.SequenceEqual(IndexKeyConverter.ToByteArray(logId)))))
                .Returns(new LogLocation(0, 0));

            // Act
            var result = await _logReader.FindLastestLog(contextName, tableName, contextIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.Id, Is.EqualTo(logId));
            Assert.That(result.Value.TupleData, Is.EqualTo(fakeData1));
            Assert.That(result.Value.IsDeleted, Is.False);
        }

        [Test]
        public async Task FindLastestLog_ReturnsLatestLog_WhenMultipleUpdates()
        {
            var contextIndex = new object[] { 1 };
            var logId = 67890;

            // Act
            _logWriter.Put(contextName, tableName, contextIndex, fakeData1, logId: logId);
            _logWriter.Put(contextName, tableName, contextIndex, fakeData2, logId: logId);
            var result = await _logReader.FindLastestLog(contextName, tableName, contextIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.TupleData, Is.EqualTo(fakeData2));
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
