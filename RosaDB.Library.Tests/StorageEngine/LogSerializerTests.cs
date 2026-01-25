#nullable disable

using System.Text;
using RosaDB.Library.Models;
using RosaDB.Library.StorageEngine.Serializers;

namespace RosaDB.Library.Tests.StorageEngine
{
    [TestFixture]
    public class LogSerializerTests
    {
        private Log CreateTestLog(long id, bool isDeleted, byte[] tupleData)
        {
            return new Log
            {
                Id = id,
                IsDeleted = isDeleted,
                Date = DateTime.UtcNow,
                TupleData = tupleData
            };
        }

        [Test]
        public void Serialize_ValidLog_ReturnsCorrectBytes()
        {
            // Arrange
            var log = CreateTestLog(123, false, Encoding.UTF8.GetBytes("test data"));

            // Act
            var bytes = LogSerializer.Serialize(log);

            // Assert
            Assert.That(bytes, Is.Not.Null);
            Assert.That(bytes.Length, Is.Not.EqualTo(0));

            using var ms = new MemoryStream(bytes);
            using var reader = new BinaryReader(ms);

            var length = reader.ReadInt32();
            var id = reader.ReadInt64();
            var isDeleted = reader.ReadBoolean();
            var dateBinary = reader.ReadInt64();
            var tupleLength = reader.ReadInt32();
            var tupleData = reader.ReadBytes(tupleLength);

            Assert.That(length, Is.EqualTo(bytes.Length - 4));
            Assert.That(id, Is.EqualTo(log.Id));
            Assert.That(isDeleted, Is.EqualTo(log.IsDeleted));
            Assert.That(dateBinary, Is.EqualTo(log.Date.ToBinary()));
            Assert.That(tupleLength, Is.EqualTo(log.TupleData.Length));
            Assert.That(tupleData, Is.EqualTo(log.TupleData));
        }

        [Test]
        public void Deserialize_ValidStream_ReturnsCorrectLog()
        {
            // Arrange
            var originalLog = CreateTestLog(456, true, Encoding.UTF8.GetBytes("another test"));
            var serializedBytes = LogSerializer.Serialize(originalLog);
            using var ms = new MemoryStream(serializedBytes);

            // Act
            var deserializedLog = LogSerializer.Deserialize(ms);

            // Assert
            Assert.That(deserializedLog, Is.Not.Null);
            Assert.That(deserializedLog.Id, Is.EqualTo(originalLog.Id));
            Assert.That(deserializedLog.IsDeleted, Is.EqualTo(originalLog.IsDeleted));
            Assert.That(deserializedLog.Date.ToBinary(), Is.EqualTo(originalLog.Date.ToBinary()));
            Assert.That(deserializedLog.TupleData, Is.EqualTo(originalLog.TupleData));
        }

        [Test]
        public async Task DeserializeAsync_ValidStream_ReturnsCorrectLog()
        {
            // Arrange
            var originalLog = CreateTestLog(789, false, Encoding.UTF8.GetBytes("async test data"));
            var serializedBytes = LogSerializer.Serialize(originalLog);
            using var ms = new MemoryStream(serializedBytes);

            // Act
            var deserializedLog = await LogSerializer.DeserializeAsync(ms);

            // Assert
            Assert.That(deserializedLog, Is.Not.Null);
            Assert.That(deserializedLog.Id, Is.EqualTo(originalLog.Id));
            Assert.That(deserializedLog.IsDeleted, Is.EqualTo(originalLog.IsDeleted));
            Assert.That(deserializedLog.Date.ToBinary(), Is.EqualTo(originalLog.Date.ToBinary()));
            Assert.That(deserializedLog.TupleData, Is.EqualTo(originalLog.TupleData));
        }

        [Test]
        public void Deserialize_EmptyStream_ReturnsNull()
        {
            // Arrange
            using var ms = new MemoryStream();

            // Act
            var deserializedLog = LogSerializer.Deserialize(ms);

            // Assert
            Assert.That(deserializedLog, Is.Null);
        }

        [Test]
        public async Task DeserializeAsync_EmptyStream_ReturnsNull()
        {
            // Arrange
            using var ms = new MemoryStream();

            // Act
            var deserializedLog = await LogSerializer.DeserializeAsync(ms);

            // Assert
            Assert.That(deserializedLog, Is.Null);
        }

        [Test]
        public void Deserialize_PartialLog_ReturnsNull()
        {
            // Arrange
            var originalLog = CreateTestLog(101, true, Encoding.UTF8.GetBytes("short data"));
            var serializedBytes = LogSerializer.Serialize(originalLog);
            
            using var ms = new MemoryStream(serializedBytes, 0, serializedBytes.Length - 5); 

            // Act
            var deserializedLog = LogSerializer.Deserialize(ms);

            // Assert
            Assert.That(deserializedLog, Is.Null);
        }

        [Test]
        public async Task DeserializeAsync_PartialLog_ReturnsNull()
        {
            // Arrange
            var originalLog = CreateTestLog(102, false, Encoding.UTF8.GetBytes("async short data"));
            var serializedBytes = LogSerializer.Serialize(originalLog);
            
            using var ms = new MemoryStream(serializedBytes, 0, serializedBytes.Length - 5); 

            // Act
            var deserializedLog = await LogSerializer.DeserializeAsync(ms);

            // Assert
            Assert.That(deserializedLog, Is.Null);
        }
    }
}