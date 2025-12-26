#nullable disable

using NUnit.Framework;
using RosaDB.Library.StorageEngine;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO;
using System.Text;

namespace RosaDB.Library.Tests
{
    [TestFixture]
    public class IndexSerializerTests
    {
        [Test]
        public void IndexHeader_SerializeAndDeserialize_ReturnsOriginalHeader()
        {
            // Arrange
            var originalHeader = new IndexHeader("TestCell", "TestTable", "InstanceHash123", 5, 2);

            // Act
            var serializedBytes = IndexSerializer.Serialize(originalHeader);
            using var ms = new MemoryStream(serializedBytes);
            var deserializedHeader = IndexSerializer.DeserializeHeader(ms);

            // Assert
            Assert.That(deserializedHeader, Is.Not.Null);
            Assert.That(deserializedHeader.Value.CellName, Is.EqualTo(originalHeader.CellName));
            Assert.That(deserializedHeader.Value.TableName, Is.EqualTo(originalHeader.TableName));
            Assert.That(deserializedHeader.Value.InstanceHash, Is.EqualTo(originalHeader.InstanceHash));
            Assert.That(deserializedHeader.Value.SegmentNumber, Is.EqualTo(originalHeader.SegmentNumber));
            Assert.That(deserializedHeader.Value.Version, Is.EqualTo(originalHeader.Version));
        }

        [Test]
        public void IndexHeader_DeserializeHeader_EmptyStream_ReturnsNull()
        {
            // Arrange
            using var ms = new MemoryStream();

            // Act
            var deserializedHeader = IndexSerializer.DeserializeHeader(ms);

            // Assert
            Assert.That(deserializedHeader, Is.Null);
        }

        [Test]
        public void IndexHeader_DeserializeHeader_PartialStream_ReturnsNull()
        {
            // Arrange - Create a stream with insufficient data for a full header
            var partialBytes = Encoding.UTF8.GetBytes("TooShort"); // Much shorter than a full header
            using var ms = new MemoryStream(partialBytes);

            // Act
            var deserializedHeader = IndexSerializer.DeserializeHeader(ms);

            // Assert
            Assert.That(deserializedHeader, Is.Null);
        }

        [Test]
        public void SparseIndexEntry_SerializeAndDeserialize_ReturnsOriginalEntry()
        {
            // Arrange
            var originalEntry = new SparseIndexEntry(1234567890L, 9876543210L, 3);

            // Act
            var serializedBytes = IndexSerializer.Serialize(originalEntry);
            using var ms = new MemoryStream(serializedBytes);
            var deserializedEntry = IndexSerializer.DeserializeEntry(ms);

            // Assert
            Assert.That(deserializedEntry, Is.Not.Null);
            Assert.That(deserializedEntry.Value.Key, Is.EqualTo(originalEntry.Key));
            Assert.That(deserializedEntry.Value.Offset, Is.EqualTo(originalEntry.Offset));
            Assert.That(deserializedEntry.Value.Version, Is.EqualTo(originalEntry.Version));
        }

        [Test]
        public void SparseIndexEntry_DeserializeEntry_EmptyStream_ReturnsNull()
        {
            // Arrange
            using var ms = new MemoryStream();

            // Act
            var deserializedEntry = IndexSerializer.DeserializeEntry(ms);

            // Assert
            Assert.That(deserializedEntry, Is.Null);
        }

        [Test]
        public void SparseIndexEntry_DeserializeEntry_PartialStream_ReturnsNull()
        {
            // Arrange - SparseIndexEntry is fixed 20 bytes. Create a shorter stream.
            var partialBytes = new byte[10];
            using var ms = new MemoryStream(partialBytes);

            // Act
            var deserializedEntry = IndexSerializer.DeserializeEntry(ms);

            // Assert
            Assert.That(deserializedEntry, Is.Null);
        }
    }
}
