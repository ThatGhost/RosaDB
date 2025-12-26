#nullable disable

using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
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

            // Setup mock database
            var mockDatabase = Database.Create("TestDb").Value;
            _mockSessionState.Setup(s => s.CurrentDatabase).Returns(mockDatabase);
            _mockFolderManager.Setup(f => f.BasePath).Returns(@"C:\Test");

            _logManager = new LogManager(_mockLogCondenser.Object, _mockSessionState.Object, _mockFileSystem, _mockFolderManager.Object);
        }

        [Test]
        public async Task GetAllLogsForCellInstanceTable_ReturnsOnlyLogsForInstance()
        {
            // Arrange
            var tableIndex1 = new object[] { 1 };
            var logId1 = 12345;
            var data1 = new byte[] { 1, 1, 1 };
            var log1 = new Log { Id = logId1, TupleData = data1 };

            var tableIndex2 = new object[] { 2 };
            var logId2 = 54321;
            var data2 = new byte[] { 2, 2, 2 };
            var log2 = new Log { Id = logId2, TupleData = data2 };

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>()))
                .Returns((Queue<Log> q) => q.ToList());

            _logManager.Put(cellName, tableName, tableIndex1, data1, logId1);
            await _logManager.Commit();
            _logManager.Put(cellName, tableName, tableIndex2, data2, logId2);
            await _logManager.Commit();

            var newLogManager = new LogManager(_mockLogCondenser.Object, _mockSessionState.Object, _mockFileSystem, _mockFolderManager.Object);
            await newLogManager.LoadIndexesAsync();

            // Act
            var allLogs = new List<Log>();
            await foreach (var log in newLogManager.GetAllLogsForCellInstanceTable(cellName, tableName, tableIndex1))
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
            // Arrange
            var tableIndex1 = new object[] { 1 };
            var logId1 = 12345;
            var data1 = new byte[] { 1, 1, 1 };
            var log1 = new Log { Id = logId1, TupleData = data1 };

            var tableIndex2 = new object[] { 2 };
            var logId2 = 54321;
            var data2 = new byte[] { 2, 2, 2 };
            var log2 = new Log { Id = logId2, TupleData = data2 };

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>()))
                .Returns((Queue<Log> q) => q.ToList());

            _logManager.Put(cellName, tableName, tableIndex1, data1, logId1);
            await _logManager.Commit();
            _logManager.Put(cellName, tableName, tableIndex2, data2, logId2);
            await _logManager.Commit();

            var newLogManager = new LogManager(_mockLogCondenser.Object, _mockSessionState.Object, _mockFileSystem, _mockFolderManager.Object);
            await newLogManager.LoadIndexesAsync();

            // Act
            var allLogs = new List<Log>();
            await foreach (var log in newLogManager.GetAllLogsForCellTable(cellName, tableName))
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
            // Arrange
            var tableIndex = new object[] { 1 };
            var logId = 12345;
            var data = new byte[] { 1, 2, 3 };
            var log = new Log { Id = logId, TupleData = data };
            var logs = new Queue<Log>();
            logs.Enqueue(log);

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>())).Returns(new List<Log> { log });

            _logManager.Put(cellName, tableName, tableIndex, data, logId);
            await _logManager.Commit();

            var newLogManager = new LogManager(_mockLogCondenser.Object, _mockSessionState.Object, _mockFileSystem, _mockFolderManager.Object);
            await newLogManager.LoadIndexesAsync();

            // Act
            var result = await newLogManager.FindLastestLog(cellName, tableName, tableIndex, logId);

            // Assert
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Value, Is.Not.Null);
            Assert.That(result.Value.Id, Is.EqualTo(logId));
            Assert.That(result.Value.TupleData, Is.EqualTo(data));
            Assert.That(result.Value.IsDeleted, Is.False);
        }

        [Test]
        public async Task LoadIndexesAsync_HandlesEmptyIndexFiles()
        {
            // Arrange
            var tableIndex = new object[] { 1 };
            var expectedPath = GetExpectedSegmentFilePath(tableIndex, 0);
            var expectedIndexPath = _mockFileSystem.Path.ChangeExtension(expectedPath, ".idx");
            _mockFileSystem.AddFile(expectedIndexPath, new MockFileData(""));

            var newLogManager = new LogManager(_mockLogCondenser.Object, _mockSessionState.Object, _mockFileSystem, _mockFolderManager.Object);

            // Act
            await newLogManager.LoadIndexesAsync();

            // Assert
            var sparseIndexCache = (IDictionary)typeof(LogManager)
                .GetField("_sparseIndexCache", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .GetValue(newLogManager);

            Assert.That(sparseIndexCache.Count, Is.EqualTo(0));
        }

        [Test]
        public async Task LoadIndexesAsync_DoesNothing_WhenDatabaseNotSet()
        {
            // Arrange
            _mockSessionState.Setup(s => s.CurrentDatabase).Returns((Database)null);

            var newLogManager = new LogManager(_mockLogCondenser.Object, _mockSessionState.Object, _mockFileSystem, _mockFolderManager.Object);

            // Act
            await newLogManager.LoadIndexesAsync();

            // Assert
            var sparseIndexCache = (IDictionary)typeof(LogManager)
                .GetField("_sparseIndexCache", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .GetValue(newLogManager);

            Assert.That(sparseIndexCache.Count, Is.EqualTo(0));
        }

        [Test]
        public async Task LoadIndexesAsync_LoadsIndexesFromDisk()
        {
            // Arrange
            var tableIndex = new object[] { 1 };
            var logId = 12345;
            var data = new byte[] { 1, 2, 3 };
            var log = new Log { Id = logId, TupleData = data };
            var logs = new Queue<Log>();
            logs.Enqueue(log);

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>())).Returns(new List<Log> { log });

            _logManager.Put(cellName, tableName, tableIndex, data, logId);
            await _logManager.Commit();

            var newLogManager = new LogManager(_mockLogCondenser.Object, _mockSessionState.Object, _mockFileSystem, _mockFolderManager.Object);

            // Act
            await newLogManager.LoadIndexesAsync();

            // Assert
            var sparseIndexCache = (IDictionary)typeof(LogManager)
                .GetField("_sparseIndexCache", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .GetValue(newLogManager);

            Assert.That(sparseIndexCache.Count, Is.EqualTo(1));
        }

        [Test]
        public async Task Commit_WritesLogsToDisk()
        {
            // Arrange
            var tableIndex = new object[] { 1 };
            var logId = 12345;
            var data = new byte[] { 1, 2, 3 };
            var log = new Log { Id = logId, TupleData = data };
            var logs = new Queue<Log>();
            logs.Enqueue(log);

            _mockLogCondenser.Setup(c => c.Condense(It.IsAny<Queue<Log>>())).Returns(new List<Log> { log });

            _logManager.Put(cellName, tableName, tableIndex, data, logId);

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
            // Arrange
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

        [Test]
        public async Task Put_AddsLogToWriteAheadLog()
        {
            // Arrange
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
            // Arrange
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
            // Arrange
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