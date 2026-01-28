#nullable disable

using System.IO.Abstractions.TestingHelpers;
using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.MoqQueries;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.Tests.MoqQueries;
using RosaDB.Library.Websockets.Interfaces;

namespace RosaDB.Library.Tests.StorageEngine
{
    [TestFixture]
    public class IntegrationTests
    {
        private MockFileSystem _mockFileSystem;

        [SetUp]
        public void Setup()
        {
            _mockFileSystem = new MockFileSystem();
        }

        [Test]
        public async Task Full_IntegrationTestFlow()
        {
            var tempDirectory = Path.Combine(Path.GetTempPath(), "rosadb_test_" + Path.GetRandomFileName());
            
            var fileSystem = _mockFileSystem; 
            var folderManager = new FolderManager(fileSystem) { BasePath = tempDirectory };
            var sessionState = new SessionState();
            var writeAheadLogCache = new WriteAheadLogCache();
            
            Mock<IIndexManager> mockIndexManager = new Mock<IIndexManager>();
            IIndexManager indexManager = mockIndexManager.Object;

            IContextManager realContextManager = new ContextManager(sessionState, fileSystem, folderManager, indexManager);
            IDatabaseManager databaseManager = new DatabaseManager(sessionState, realContextManager, fileSystem, folderManager);
            var rootManager = new RootManager(databaseManager, sessionState, fileSystem, folderManager);
            var mockSubscriptionManager = new Mock<ISubscriptionManager>();
            var logWriter = new LogWriter(new LogCondenser(), sessionState, fileSystem, folderManager, indexManager, realContextManager, mockSubscriptionManager.Object, writeAheadLogCache);
            var logReader = new LogReader(sessionState, fileSystem, folderManager, indexManager, writeAheadLogCache);
            
            var createDbQuery = new CreateDatabaseQuery(rootManager);
            var useDbQuery = new UseDatabaseQuery(rootManager, sessionState);
            var createContextQuery = new CreateContextQuery(databaseManager);
            var createTableQuery = new CreateTableDefinition(realContextManager);
            var writeQuery = new WriteLogAndCommitQuery(logWriter, realContextManager);
            var updateQuery = new UpdateContextLogsQuery(logReader, logWriter, realContextManager);
            var getQuery = new GetContextLogsQuery(logReader, realContextManager);

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
    }
}
