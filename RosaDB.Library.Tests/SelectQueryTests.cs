#nullable disable

using Moq;
using NUnit.Framework;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Models.Environments;
using RosaDB.Library.Query.Queries;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions.TestingHelpers;

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
            Column.Create("id", DataType.BIGINT, isPrimaryKey: true).Value
        ];
        
        private readonly Column[] cellColumns =
        [
            Column.Create("cellId", DataType.BIGINT, isIndex: true).Value
        ];

        private byte[] fakeData1;
        private byte[] fakeData2;
        private Log fakeLog1;
        private Log fakeLog2;
        private Row fakeCellInstance1;
        
        [SetUp]
        public void Setup()
        {
            _mockCellManager = new Mock<ICellManager>(); 
            _mockLogManager = new Mock<ILogManager>(); 
            
            _mockCellManager.Setup(cm => cm.GetColumnsFromTable(cellName, tableName)).ReturnsAsync(tableColumns);
            
            // Setup Environment for USING clause (needs to know about indexes)
            var cellEnv = new CellEnvironment
            {
                Columns = cellColumns
            };
            _mockCellManager.Setup(cm => cm.GetEnvironment(cellName)).ReturnsAsync(cellEnv);
            
            fakeData1 = RowSerializer.Serialize(Row.Create([(long)1, "data1"], tableColumns).Value).Value;
            fakeData2 = RowSerializer.Serialize(Row.Create([(long)2, "data2"], tableColumns).Value).Value;
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
            fakeCellInstance1 = Row.Create([1],cellColumns).Value;
            _mockCellManager.Setup(c => c.GetCellInstance(cellName, It.IsAny<string>())).Returns(() => Task.FromResult(fakeCellInstance1));
        }

        [Test]
        public async Task SELECT_AllRowsInTable()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellTable(cellName, tableName)).Returns(() => new List<Log>()
            {
                fakeLog1, fakeLog2
            });
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
        public async Task SELECT_AllRowsInCellTable()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { 2 })).Returns(() => new List<Log>()
            {
                fakeLog1
            });
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { 1 })).Returns(() => new List<Log>()
            {
                fakeLog2
            });
            
            string[] tokens = ["SELECT", "*", "FROM", $"{cellName}.{tableName}", "USING", "cellId", "=", "1",";"];
            
            // Act
            var query = new SelectQuery(tokens, _mockLogManager.Object, _mockCellManager.Object);
            var result = await query.Execute();
            
            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Rows, Is.Not.Null);
            Assert.That(result.Rows.Count, Is.EqualTo(1));
        }
        
        [Test]
        public async Task SELECT_WithWHERE_ShouldReturnRowsAcrossCellsWithCorrectWhere()
        {
            // Arrange
            _mockLogManager.Setup(l => l.GetAllLogsForCellTable(cellName, tableName)).Returns(() => new List<Log>()
            {
                fakeLog1, fakeLog2
            });
            
            string[] tokens = ["SELECT", "*", "FROM", $"{cellName}.{tableName}", "WHERE", "data", "=", "data1",";"];
            
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
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { 2 })).Returns(() => new List<Log>()
            {
                fakeLog1
            });
            _mockLogManager.Setup(l => l.GetAllLogsForCellInstanceTable(cellName, tableName, new object[] { 1 })).Returns(() => new List<Log>()
            {
                fakeLog2,
                new()
                {
                    Id = 3,
                    Date = DateTime.Now,
                    IndexValues = null,
                    IsDeleted = false,
                    TupleData = RowSerializer.Serialize(Row.Create([(long)3, "data3"], tableColumns).Value).Value
                }
            });
            
            string[] tokens = ["SELECT", "*", "FROM", $"{cellName}.{tableName}", "USING", "cellId", "=", "1", "WHERE", "data", "=", "data2",";"];
            
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