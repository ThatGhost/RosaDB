#nullable disable

using Moq;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using System.IO;
using System.IO.Abstractions;
using System.Security.Cryptography;
using System.Text;
using NUnit.Framework;

namespace RosaDB.Library.Tests
{
    [TestFixture]
    public class IndexManagerTests
    {
        private IFileSystem _fileSystem;
        private Mock<IFolderManager> _mockFolderManager;
        private IndexManager _indexManager;
        private string _tempDirectory;

        private string cellName = "TestCell";
        private string tableName = "TestTable";
        private object[] tableIndex = new object[] { 1 };
        private string columnName = "LogId";

        [SetUp]
        public void Setup()
        {
            _fileSystem = new FileSystem();
            _tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            _mockFolderManager = new Mock<IFolderManager>();
            _mockFolderManager.Setup(f => f.BasePath).Returns(_tempDirectory);

            _indexManager = new IndexManager(_fileSystem, _mockFolderManager.Object);
        }

        private TableInstanceIdentifier CreateIdentifier(string cellName, string tableName, object[] indexValues)
        {
            var indexString = string.Join(";", indexValues.Select(v => v.ToString()));
            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(indexString)));
            return new TableInstanceIdentifier(cellName, tableName, hash);
        }

        [Test]
        public void GetOrCreateBPlusTree_CreatesIndexFile()
        {
            // Arrange
            var identifier = CreateIdentifier(cellName, tableName, tableIndex);
            var expectedIndexFilePath = _fileSystem.Path.Combine(
                _mockFolderManager.Object.BasePath,
                "indexes",
                identifier.CellName,
                identifier.TableName,
                identifier.InstanceHash.Substring(0, 2),
                $"{identifier.InstanceHash}_{columnName}.idx");

            // Act
            _indexManager.GetOrCreateBPlusTree(identifier, columnName);

            // Assert
            Assert.That(_fileSystem.File.Exists(expectedIndexFilePath), Is.True);
        }

        [TearDown]
        public void Teardown()
        {
            _indexManager.CloseAllIndexes();
            if (Directory.Exists(_tempDirectory))
            {
                Directory.Delete(_tempDirectory, true);
            }
        }
    }
}