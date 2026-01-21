namespace RosaDB.Library.Query.TokenParsers;

public static class TokenToKeywordIndex
{
    public static int Find(string[] tokens, string keyword, int startIndex = 0)
    {
        for (int i = startIndex; i < tokens.Length; i++)
            if (tokens[i].Equals(keyword, StringComparison.OrdinalIgnoreCase)) return i;
        return -1;
    }
}