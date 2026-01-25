#nullable disable

using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Tests.StorageEngine
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
            // First byte is null bitmap (0 = not null), then follows the INT data which should be 4 bytes
            var corruptedData = new byte[] { 0, 1, 2 }; // Not enough bytes for an INT (needs 4 bytes + 1 byte bitmap)

            // Act
            var result = RowSerializer.Deserialize(corruptedData, columns);

            // Assert
            Assert.That(result.IsFailure, Is.True);
            // The generic catch-all turns it into a CriticalError or DataError depending on implementation
            // Since we catch EndOfStreamException and return DataError now:
            Assert.That(result.Error.Prefix, Is.EqualTo(RosaDB.Library.Core.ErrorPrefixes.DataError).Or.EqualTo(RosaDB.Library.Core.ErrorPrefixes.CriticalError));
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
            Assert.That(result.Error.Message, Does.Contain("Unknown or unsupported data type"));
        }
    }
}
