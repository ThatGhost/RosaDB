using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using System.IO.Abstractions.TestingHelpers;

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
            _mockFolderManager.Setup(f => f.BasePath).Returns("C:\\Test");

            _logManager = new LogManager(_mockLogCondenser.Object, _mockSessionState.Object, _mockFileSystem, _mockFolderManager.Object);
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