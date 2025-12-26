#nullable disable

using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.Collections;
using System.IO.Abstractions.TestingHelpers;
using System.Security.Cryptography;
using System.Text;

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