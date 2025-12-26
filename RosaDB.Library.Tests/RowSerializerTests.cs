#nullable disable

using NUnit.Framework;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Serializers;
using System.Text;

namespace RosaDB.Library.Tests
{
    [TestFixture]
    public class RowSerializerTests
    {
        [Test]
        public void Serialize_And_Deserialize_ValidRow_ReturnsCorrectRow()
        {
            // Arrange
            var columns = new[]
            {
                Column.Create("Id", DataType.INT).Value,
                Column.Create("Name", DataType.VARCHAR).Value,
                Column.Create("IsActive", DataType.BOOLEAN).Value,
                Column.Create("Value", DataType.BIGINT).Value
            };

            var values = new object[] { 123, "Test Name", true, 9876543210L };
            var originalRow = Row.Create(values, columns).Value;

            // Act
            var serializeResult = RowSerializer.Serialize(originalRow);
            Assert.That(serializeResult.IsSuccess, Is.True);
            var serializedBytes = serializeResult.Value;

            var deserializeResult = RowSerializer.Deserialize(serializedBytes, columns);

            // Assert
            Assert.That(deserializeResult.IsSuccess, Is.True);
            var deserializedRow = deserializeResult.Value;

            Assert.That(deserializedRow.Values.Length, Is.EqualTo(originalRow.Values.Length));
            for (int i = 0; i < originalRow.Values.Length; i++)
            {
                Assert.That(deserializedRow.Values[i], Is.EqualTo(originalRow.Values[i]));
            }
        }

        [Test]
        public void Serialize_UnsupportedDataType_ReturnsError()
        {
            // Arrange
            var columns = new[]
            {
                Column.Create("Unsupported", DataType.FLOAT).Value
            };
            var values = new object[] { 123.45f };
            var row = Row.Create(values, columns).Value;

            // Act
            var result = RowSerializer.Serialize(row);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Does.Contain("not supported for serialization"));
        }

        [Test]
        public void Deserialize_CorruptedData_ReturnsError()
        {
            // Arrange
            var columns = new[] { Column.Create("Id", DataType.INT).Value };
            var corruptedData = new byte[] { 1, 2 }; // Not enough bytes for an INT

            // Act
            var result = RowSerializer.Deserialize(corruptedData, columns);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            // The generic catch-all turns it into a CriticalError
            Assert.That(result.Error, Is.TypeOf<RosaDB.Library.Core.CriticalError>());
        }

        [Test]
        public void Deserialize_UnsupportedDataTypeInColumns_ReturnsError()
        {
            // Arrange
            var columns = new[]
            {
                Column.Create("Id", DataType.INT).Value,
            };
            var values = new object[] { 123 };
            var originalRow = Row.Create(values, columns).Value;
            var serializedBytes = RowSerializer.Serialize(originalRow).Value;

            var unsupportedColumns = new[]
            {
                Column.Create("Id", DataType.NUMERIC).Value,
            };

            // Act
            var result = RowSerializer.Deserialize(serializedBytes, unsupportedColumns);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            Assert.That(result.Error.Message, Does.Contain("Unknown data type"));
        }
    }
}
