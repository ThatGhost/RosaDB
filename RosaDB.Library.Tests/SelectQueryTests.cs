#nullable disable

using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Query.Queries;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Tests
{
    [TestFixture]
    public class SelectQueryTests
    {
        private Mock<ICellManager> _mockCellManager;
        private Mock<ILogManager> _mockLogManager;

        private const string cellName = "TestCell";
        private const string tableName = "TestTable";

        private readonly Column[] tableColumns =
        [
            Column.Create("data", DataType.VARCHAR).Value,
            Column.Create("id", DataType.BIGINT, isPrimaryKey: true).Value,
            Column.Create("age", DataType.INT).Value,
            Column.Create("city", DataType.VARCHAR).Value
        ];

        private readonly Column[] cellColumns =
        [
            Column.Create("cellId", DataType.BIGINT, isIndex: true).Value,
            Column.Create("name", DataType.VARCHAR, isIndex: false).Value
        ];

        private byte[] fakeData1;
        private byte[] fakeData2;
        private byte[] fakeData3;
        private Log fakeLog1;
        private Log fakeLog2;
        private Log fakeLog3;
        private Row fakeCellInstance1;

        [SetUp]
        public void Setup()
        {
            _mockCellManager = new Mock<ICellManager>();
            _mockLogManager = new Mock<ILogManager>();

            _mockCellManager.Setup(cm => cm.GetColumnsFromTable(cellName, tableName)).ReturnsAsync(tableColumns);

            var cellEnv = new CellEnvironment { Columns = cellColumns };
            _mockCellManager.Setup(cm => cm.GetEnvironment(cellName)).ReturnsAsync(cellEnv);

            fakeData1 = RowSerializer.Serialize(Row.Create(["data1", (long)1, 30, "New York"], tableColumns).Value).Value;
            fakeData2 = RowSerializer.Serialize(Row.Create(["data2", (long)2, 25, "Los Angeles"], tableColumns).Value).Value;
            fakeData3 = RowSerializer.Serialize(Row.Create(["data3", (long)3, 35, "Chicago"], tableColumns).Value).Value;

            fakeLog1 = new Log()
            {
                Id = 1,
                Date = DateTime.Now,
                IndexValues = null,
                IsDeleted = false,
                TupleData = fakeData1
            };
            fakeLog2 = new Log()
            {
                Id = 2,
                Date = DateTime.Now,
                IndexValues = null,
                IsDeleted = false,
                TupleData = fakeData2
            };
            fakeLog3 = new Log()
            {
                Id = 3,
                Date = DateTime.Now,
                IndexValues = null,
                IsDeleted = false,
                TupleData = fakeData3
            };
            fakeCellInstance1 = Row.Create([1], cellColumns).Value;
            _mockCellManager.Setup(c => c.GetCellInstance(cellName, It.IsAny<string>()))
                .Returns(() => Task.FromResult(Result<Row>.Success(fakeCellInstance1)));
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        private async IAsyncEnumerable<T> ToAsyncEnumerable<T>(IEnumerable<T> source)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            foreach (var item in source)
            {
                yield return item;
            }
        }

        [Test]
        public async Task SELECT_AllRowsInTable()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellTable(cellName, tableName)).Returns(() =>
                ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2]));
            string[] tokens = ["SELECT", "*", "FROM", $"{cellName}.{tableName}", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockCellManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Rows, Is.Not.Null);
            Assert.That(result.Rows.Count, Is.EqualTo(2));
        }

        [Test]
        public async Task SELECT_WithUSING_ShouldReturnLogsInCellWithIndexedValue()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { (long)2 }))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1]));
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { (long)1 }))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog2]));

            string[] tokens = ["SELECT", "*", "FROM", $"{cellName}.{tableName}", "USING", "cellId", "=", "1", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockCellManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Rows, Is.Not.Null);
            Assert.That(result.Rows.Count, Is.EqualTo(1));
            Assert.That(result.Rows[0], Is.Not.Null);
            Assert.That(result.Rows[0].Values, Is.Not.Null);
            Assert.That(result.Rows[0].Values[0], Is.EqualTo("data2"));
        }
        
        [Test]
        public async Task SELECT_WithUSING_ShouldReturnLogsInCell()
        {
            // Arrange
            _mockCellManager.Setup(c => c.GetAllCellInstances(cellName))
                .Returns(() => Task.FromResult<Result<IEnumerable<Row>>>(
                    new[] {
                        Row.Create([(long)2, "test"], cellColumns).Value
                    }
                ));
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { (long)2 })).Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1]));
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { (long)1 })).Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog2]));

            string[] tokens = ["SELECT", "*", "FROM", $"{cellName}.{tableName}", "USING", "name", "=", "test", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockCellManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Rows, Is.Not.Null);
            Assert.That(result.Rows.Count, Is.EqualTo(1));
            Assert.That(result.Rows[0], Is.Not.Null);
            Assert.That(result.Rows[0].Values, Is.Not.Null);
            Assert.That(result.Rows[0].Values[0], Is.EqualTo("data1"));
        }

        [Test]
        public async Task SELECT_WithWHERE_ShouldReturnRowsAcrossCellsWithCorrectWhere()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellTable(cellName, tableName)).Returns(() =>
                ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2]));

            string[] tokens = ["SELECT", "*", "FROM", $"{cellName}.{tableName}", "WHERE", "age", "=", "30", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockCellManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Rows, Is.Not.Null);
            Assert.That(result.Rows.Count, Is.EqualTo(1));
        }

        [Test]
        public async Task SELECT_WithWHEREAndUSING_ShouldReturnRowsWithCorrectWhereAndUsing()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { (long)2 }))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1]));
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { (long)1 }))
                .Returns(() => ToAsyncEnumerable((List<Log>) [fakeLog2, fakeLog3]));

            string[] tokens = ["SELECT", "*", "FROM", $"{cellName}.{tableName}", "USING", "cellId", "=", "1", "WHERE", "city", "=", "Chicago", ";" ];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockCellManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Rows, Is.Not.Null);
            Assert.That(result.Rows.Count, Is.EqualTo(1));
        }

        [Test]
        public async Task SELECT_ProjectOneColumn_ShouldReturnRowsWithOneColumn()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellTable(cellName, tableName)).Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2]));
            string[] tokens = ["SELECT", "city", "FROM", $"{cellName}.{tableName}", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockCellManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Rows, Is.Not.Null);
            Assert.That(result.Rows.Count, Is.EqualTo(2));
            Assert.That(result.Rows[0].Columns.Length, Is.EqualTo(1));
            Assert.That(result.Rows[0].Columns[0].Name, Is.EqualTo("city"));
        }

        [Test]
        public async Task SELECT_ProjectMultipleColumns_ShouldReturnRowsWithMultipleColumns()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellTable(cellName, tableName)).Returns(() =>
                ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2]));
            string[] tokens = ["SELECT", "age", ",", "city", "FROM", $"{cellName}.{tableName}", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockCellManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Rows, Is.Not.Null);
            Assert.That(result.Rows.Count, Is.EqualTo(2));
            Assert.That(result.Rows[0].Columns.Length, Is.EqualTo(2));
            Assert.That(result.Rows[0].Columns[0].Name, Is.EqualTo("age"));
            Assert.That(result.Rows[0].Columns[1].Name, Is.EqualTo("city"));
        }

        [Test]
        public async Task SELECT_WithWHEREAnd_ShouldReturnRowsWithCorrectWhere()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellTable(cellName, tableName)).Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2, fakeLog3]));

            string[] tokens = ["SELECT", "*", "FROM", $"{cellName}.{tableName}", "WHERE", "age", "=", "30", "AND", "city", "=", "New York", ";" ];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockCellManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Rows, Is.Not.Null);
            Assert.That(result.Rows.Count, Is.EqualTo(1));
        }
    }
}