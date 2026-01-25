#nullable disable

using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Tests.StorageEngine;

[TestFixture]
public class RowSerializerNullableTests
{
    [Test]
    public void Serialize_And_Deserialize_WithNulls_ReturnsCorrectRow()
    {
        // Arrange
        var columns = new[]
        {
            Column.Create("Id", DataType.INT).Value,
            Column.Create("Description", DataType.VARCHAR, isNullable: true).Value,
            Column.Create("Count", DataType.INT, isNullable: true).Value,
            Column.Create("IsActive", DataType.BOOLEAN).Value
        };

#pragma warning disable IDE0300 // Simplify collection initialization
#pragma warning disable CS8632 // The annotation for nullable reference types should only be used in code within a '#nullable' annotations context.
        object[] values = new object?[] { 1, null, null, true };
#pragma warning restore CS8632 // The annotation for nullable reference types should only be used in code within a '#nullable' annotations context.
#pragma warning restore IDE0300 // Simplify collection initialization
        var originalRow = Row.Create(values, columns).Value;

        // Act
        var serializeResult = RowSerializer.Serialize(originalRow);
        Assert.That(serializeResult.IsSuccess, Is.True);
        
        var deserializeResult = RowSerializer.Deserialize(serializeResult.Value!, columns);
        
        // Assert
        Assert.That(deserializeResult.IsSuccess, Is.True);
        var deserializedRow = deserializeResult.Value;
        
        Assert.That(deserializedRow!.Values.Length, Is.EqualTo(4));
        Assert.That(deserializedRow.Values[0], Is.EqualTo(1));
        Assert.That(deserializedRow.Values[1], Is.Null);
        Assert.That(deserializedRow.Values[2], Is.Null);
        Assert.That(deserializedRow.Values[3], Is.EqualTo(true));
    }

    [Test]
    public void Serialize_And_Deserialize_MixedNulls_ReturnsCorrectRow()
    {
        // Arrange
        var columns = new[]
        {
            Column.Create("C1", DataType.INT, isNullable: true).Value,
            Column.Create("C2", DataType.INT, isNullable: true).Value,
            Column.Create("C3", DataType.INT, isNullable: true).Value
        };

#pragma warning disable CS8632 // The annotation for nullable reference types should only be used in code within a '#nullable' annotations context.
        var values = new object?[] { 10, null, 30 };
#pragma warning restore CS8632 // The annotation for nullable reference types should only be used in code within a '#nullable' annotations context.
        var originalRow = Row.Create(values, columns).Value;

        // Act
        var serializeResult = RowSerializer.Serialize(originalRow);
        Assert.That(serializeResult.IsSuccess, Is.True);

        var deserializeResult = RowSerializer.Deserialize(serializeResult.Value!, columns);

        // Assert
        Assert.That(deserializeResult.IsSuccess, Is.True);
        var deserializedRow = deserializeResult.Value;

        Assert.That(deserializedRow!.Values[0], Is.EqualTo(10));
        Assert.That(deserializedRow.Values[1], Is.Null);
        Assert.That(deserializedRow.Values[2], Is.EqualTo(30));
    }
}