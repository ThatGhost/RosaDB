#nullable disable

using NUnit.Framework;
using RosaDB.Library.Core;

namespace RosaDB.Library.Tests.Core
{
    [TestFixture]
    public class ByteArrayComparerTests
    {
        private readonly ByteArrayComparer _comparer = new();

        [Test]
        public void Test_Compare_BothArraysNull_ReturnsZero()
        {
            Assert.That(_comparer.Compare(null, null), Is.EqualTo(0));
        }

        [Test]
        public void Test_Compare_FirstArrayNull_ReturnsMinusOne()
        {
            Assert.That(_comparer.Compare(null, new byte[] { 1 }), Is.EqualTo(-1));
        }

        [Test]
        public void Test_Compare_SecondArrayNull_ReturnsOne()
        {
            Assert.That(_comparer.Compare(new byte[] { 1 }, null), Is.EqualTo(1));
        }

        [Test]
        public void Test_Compare_SameInstance_ReturnsZero()
        {
            var arr = new byte[] { 1, 2, 3 };
            Assert.That(_comparer.Compare(arr, arr), Is.EqualTo(0));
        }

        [Test]
        public void Test_Compare_EqualArrays_ReturnsZero()
        {
            var arr1 = new byte[] { 1, 2, 3 };
            var arr2 = new byte[] { 1, 2, 3 };
            Assert.That(_comparer.Compare(arr1, arr2), Is.EqualTo(0));
        }

        [Test]
        public void Test_Compare_FirstArrayShorter_ReturnsMinusOne()
        {
            var arr1 = new byte[] { 1, 2 };
            var arr2 = new byte[] { 1, 2, 3 };
            Assert.That(_comparer.Compare(arr1, arr2), Is.EqualTo(-1));
        }

        [Test]
        public void Test_Compare_SecondArrayShorter_ReturnsOne()
        {
            var arr1 = new byte[] { 1, 2, 3 };
            var arr2 = new byte[] { 1, 2 };
            Assert.That(_comparer.Compare(arr1, arr2), Is.EqualTo(1));
        }

        [Test]
        public void Test_Compare_LexicographicallySmaller_ReturnsMinusOne()
        {
            var arr1 = new byte[] { 1, 2, 3 };
            var arr2 = new byte[] { 1, 2, 4 };
            Assert.That(_comparer.Compare(arr1, arr2), Is.EqualTo(-1));
        }

        [Test]
        public void Test_Compare_LexicographicallyLarger_ReturnsOne()
        {
            var arr1 = new byte[] { 1, 2, 4 };
            var arr2 = new byte[] { 1, 2, 3 };
            Assert.That(_comparer.Compare(arr1, arr2), Is.EqualTo(1));
        }

        [Test]
        public void Test_Compare_EmptyArrays_ReturnsZero()
        {
            var arr1 = new byte[0];
            var arr2 = new byte[0];
            Assert.That(_comparer.Compare(arr1, arr2), Is.EqualTo(0));
        }

        [Test]
        public void Test_Compare_FirstEmpty_ReturnsMinusOne()
        {
            var arr1 = new byte[0];
            var arr2 = new byte[] { 1 };
            Assert.That(_comparer.Compare(arr1, arr2), Is.EqualTo(-1));
        }

        [Test]
        public void Test_Compare_SecondEmpty_ReturnsOne()
        {
            var arr1 = new byte[] { 1 };
            var arr2 = new byte[0];
            Assert.That(_comparer.Compare(arr1, arr2), Is.EqualTo(1));
        }
    }
}
