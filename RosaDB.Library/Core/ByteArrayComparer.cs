namespace RosaDB.Library.Core;

public class ByteArrayComparer : IComparer<byte[]>
{
    public int Compare(byte[]? x, byte[]? y)
    {
        if (x == null || y == null)
        {
            return x == y ? 0 : (x == null ? -1 : 1);
        }

        int len = Math.Min(x.Length, y.Length);
        for (int i = 0; i < len; i++)
        {
            int c = x[i].CompareTo(y[i]);
            if (c != 0) return c;
        }

        return x.Length.CompareTo(y.Length);
    }
}
