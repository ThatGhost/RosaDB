namespace RosaDB.Library.Query;

public static class DataComparer
{
    public static bool CompareEquals(object? value1, object? value2)
    {
        if (value1 == null && value2 == null) return true;
        else if (value1 == null) return false;
        return value1.Equals(value2);
    }

    public static bool CompareGreaterThan(object? value1, object? value2)
    {
        if (value1 == null || value2 == null) return false;
        return ((IComparable)value1).CompareTo(value2) > 0;
    }

    public static bool CompareLessThan(object? value1, object? value2)
    {
        if (value1 == null || value2 == null) return false;
        return ((IComparable)value1).CompareTo(value2) < 0;
    }

    public static bool CompareGreaterThanOrEqual(object? value1, object? value2)
    {
        if (value1 == null || value2 == null) return false;
        return ((IComparable)value1).CompareTo(value2) >= 0;
    }

    public static bool CompareLessThanOrEqual(object? value1, object? value2)
    {
        if (value1 == null || value2 == null) return false;
        return ((IComparable)value1).CompareTo(value2) <= 0;
    }
}
