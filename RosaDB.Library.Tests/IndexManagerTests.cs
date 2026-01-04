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

        [TearDown]
        public void Teardown()
        {
            _indexManager.Dispose();
            if (Directory.Exists(_tempDirectory))
            {
                Directory.Delete(_tempDirectory, true);
            }
        }
    }
}