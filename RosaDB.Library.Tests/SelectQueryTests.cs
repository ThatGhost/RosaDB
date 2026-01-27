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
        private Mock<IContextManager> _mockContextManager;
        private Mock<ILogManager> _mockLogManager;

        private const string contextName = "TestContext";
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
        private Row fakeContextInstance1;

        private bool AreObjectArraysEqual(object?[] a1, object?[] a2)
        {
            Console.WriteLine($"Comparing a1: [{string.Join(", ", a1 ?? new object[] { "null" })}] with a2: [{string.Join(", ", a2 ?? new object[] { "null" })}]");
            if (ReferenceEquals(a1, a2)) return true;
            if (a1 == null || a2 == null) return false;
            if (a1.Length != a2.Length) return false;
            for (int i = 0; i < a1.Length; i++)
            {
                if (!Equals(a1[i], a2[i]))
                {
                    Console.WriteLine($"Mismatch at index {i}: a1[{i}]={a1[i]} ({a1[i]?.GetType().Name}) vs a2[{i}]={a2[i]} ({a2[i]?.GetType().Name})");
                    return false;
                }
            }
            return true;
        }

        [SetUp]
        public void Setup()
        {
            _mockContextManager = new Mock<IContextManager>();
            _mockLogManager = new Mock<ILogManager>();

            _mockContextManager.Setup(cm => cm.GetColumnsFromTable(contextName, tableName)).ReturnsAsync(tableColumns);

            var cellEnv = new ContextEnvironment { Columns = cellColumns };
            _mockContextManager.Setup(cm => cm.GetEnvironment(contextName)).ReturnsAsync(cellEnv);

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
            fakeContextInstance1 = Row.Create([1, "test"], cellColumns).Value;
            _mockContextManager.Setup(c => c.GetContextInstance(contextName, It.IsAny<string>())).Returns(() => Task.FromResult(Result<Row>.Success(fakeContextInstance1)));
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
        
        private async Task<List<T>> ToList<T>(IAsyncEnumerable<T> source)
        {
            List<T> result = [];
            await foreach (var item in source)
            {
                result.Add(item);
            }

            return result;
        }

        [Test]
        public async Task SELECT_AllRowsInTable()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForContextTable(contextName, tableName)).Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2]));
            string[] tokens = ["SELECT", "*", "FROM", $"{contextName}.{tableName}", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockContextManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            var rows = await ToList(result.RowStream);
            Assert.That(rows, Is.Not.Null);
            Assert.That(rows.Count, Is.EqualTo(2));
        }

        [Test]
        public async Task SELECT_WithUSING_ShouldReturnLogsInContextWithIndexedValue()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForContextInstanceTable(
                contextName, 
                tableName, 
                It.Is<object?[]>(arr => AreObjectArraysEqual(arr, new object?[] { (long)2 }))))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1]));
            _mockLogManager.Setup(l => l.GetAllLogsForContextInstanceTable(
                contextName, 
                tableName, 
                It.Is<object?[]>(arr => AreObjectArraysEqual(arr, new object?[] { (long)1 }))))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog2]));

            string[] tokens = ["SELECT", "*", "FROM", $"{contextName}.{tableName}", "USING", "cellId", "=", "1", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockContextManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            var rows = await ToList(result.RowStream);
            Assert.That(rows, Is.Not.Null);
            Assert.That(rows.Count, Is.EqualTo(1));
            Assert.That(rows[0], Is.Not.Null);
            Assert.That(rows[0].Values, Is.Not.Null);
            Assert.That(rows[0].Values[0], Is.EqualTo("data2"));
        }
        
        [Test]
        public async Task SELECT_WithUSING_ShouldReturnLogsInContext()
        {
            // Arrange
            _mockContextManager.Setup(c => c.GetAllContextInstances(contextName))
                .Returns(() => Task.FromResult<Result<IEnumerable<Row>>>(
                    new[] {
                        Row.Create([(long)2, "test"], cellColumns).Value
                    }
                ));
            _mockLogManager.Setup(l => l.GetAllLogsForContextInstanceTable(
                contextName, 
                tableName, 
                It.Is<object?[]>(arr => AreObjectArraysEqual(arr, new object?[] { (long)2 }))))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1]));
            _mockLogManager.Setup(l => l.GetAllLogsForContextInstanceTable(
                contextName, 
                tableName, 
                It.Is<object?[]>(arr => AreObjectArraysEqual(arr, new object?[] { (long)1 }))))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog2]));

            string[] tokens = ["SELECT", "*", "FROM", $"{contextName}.{tableName}", "USING", "name", "=", "test", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockContextManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            var rows = await ToList(result.RowStream);
            Assert.That(rows, Is.Not.Null);
            Assert.That(rows.Count, Is.EqualTo(1));
            Assert.That(rows[0], Is.Not.Null);
            Assert.That(rows[0].Values, Is.Not.Null);
            Assert.That(rows[0].Values[0], Is.EqualTo("data1"));
        }
        
        [Test]
        public async Task SELECT_WithUSINGAndAND_ShouldReturnLogsInContext()
        {
            // Arrange
            _mockContextManager.Setup(c => c.GetAllContextInstances(contextName)).Returns(() => Task.FromResult<Result<IEnumerable<Row>>>(new[] { Row.Create([(long)1, "test"], cellColumns).Value } ));
            _mockLogManager.Setup(l => l.GetAllLogsForContextInstanceTable(
                contextName, 
                tableName, 
                It.Is<object?[]>(arr => AreObjectArraysEqual(arr, new object?[] { (long)2 }))))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1]));
            _mockLogManager.Setup(l => l.GetAllLogsForContextInstanceTable(
                contextName, 
                tableName, 
                It.Is<object?[]>(arr => AreObjectArraysEqual(arr, new object?[] { (long)1 }))))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog2]));

            string[] tokens = ["SELECT", "*", "FROM", $"{contextName}.{tableName}", "USING", "name", "=", "test", "AND", "cellId", "=", "1", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockContextManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            var rows = await ToList(result.RowStream);
            Assert.That(rows, Is.Not.Null);
            Assert.That(rows.Count, Is.EqualTo(1));
            Assert.That(rows[0], Is.Not.Null);
            Assert.That(rows[0].Values, Is.Not.Null);
            Assert.That(rows[0].Values[0], Is.EqualTo("data2"));
        }

        [Test]
        public async Task SELECT_WithWHERE_ShouldReturnRowsAcrossContextsWithCorrectWhere()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForContextTable(contextName, tableName)).Returns(() =>
                ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2]));

            string[] tokens = ["SELECT", "*", "FROM", $"{contextName}.{tableName}", "WHERE", "age", "=", "30", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockContextManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            var rows = await ToList(result.RowStream);
            Assert.That(rows, Is.Not.Null);
            Assert.That(rows.Count, Is.EqualTo(1));
        }

        [Test]
        public async Task SELECT_WithWHEREAndUSING_ShouldReturnRowsWithCorrectWhereAndUsing()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForContextInstanceTable(
                contextName, 
                tableName, 
                It.Is<object?[]>(arr => AreObjectArraysEqual(arr, new object?[] { (long)2 }))))
                .Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1]));
            _mockLogManager.Setup(l => l.GetAllLogsForContextInstanceTable(
                contextName, 
                tableName, 
                It.Is<object?[]>(arr => AreObjectArraysEqual(arr, new object?[] { (long)1 }))))
                .Returns(() => ToAsyncEnumerable((List<Log>) [fakeLog2, fakeLog3]));

            string[] tokens = ["SELECT", "*", "FROM", $"{contextName}.{tableName}", "USING", "cellId", "=", "1", "WHERE", "city", "=", "Chicago", ";" ];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockContextManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            var rows = await ToList(result.RowStream);
            Assert.That(rows, Is.Not.Null);
            Assert.That(rows.Count, Is.EqualTo(1));
        }

        [Test]
        public async Task SELECT_ProjectOneColumn_ShouldReturnRowsWithOneColumn()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForContextTable(contextName, tableName)).Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2]));
            string[] tokens = ["SELECT", "city", "FROM", $"{contextName}.{tableName}", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockContextManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            var rows = await ToList(result.RowStream);
            Assert.That(rows, Is.Not.Null);
            Assert.That(rows.Count, Is.EqualTo(2));
            Assert.That(rows[0].Columns.Length, Is.EqualTo(1));
            Assert.That(rows[0].Columns[0].Name, Is.EqualTo("city"));
        }

        [Test]
        public async Task SELECT_ProjectMultipleColumns_ShouldReturnRowsWithMultipleColumns()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForContextTable(contextName, tableName)).Returns(() =>
                ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2]));
            string[] tokens = ["SELECT", "age", ",", "city", "FROM", $"{contextName}.{tableName}", ";"];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockContextManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            var rows = await ToList(result.RowStream);
            Assert.That(rows, Is.Not.Null);
            Assert.That(rows.Count, Is.EqualTo(2));
            Assert.That(rows[0].Columns.Length, Is.EqualTo(2));
            Assert.That(rows[0].Columns[0].Name, Is.EqualTo("age"));
            Assert.That(rows[0].Columns[1].Name, Is.EqualTo("city"));
        }

        [Test]
        public async Task SELECT_WithWHEREAnd_ShouldReturnRowsWithCorrectWhere()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForContextTable(contextName, tableName)).Returns(() => ToAsyncEnumerable((List<Log>)[fakeLog1, fakeLog2, fakeLog3]));

            string[] tokens = ["SELECT", "*", "FROM", $"{contextName}.{tableName}", "WHERE", "age", "=", "30", "AND", "city", "=", "New York", ";" ];

            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockContextManager.Object);
            var result = await query.Execute();

            // Assert
            Assert.That(result, Is.Not.Null);
            var rows = await ToList(result.RowStream);
            Assert.That(rows, Is.Not.Null);
            Assert.That(rows.Count, Is.EqualTo(1));
        }
    }
}