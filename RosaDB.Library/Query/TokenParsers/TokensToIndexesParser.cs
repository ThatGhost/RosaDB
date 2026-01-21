namespace RosaDB.Library.Query.TokenParsers;

public static class TokensToIndexesParser
{
    public static (int selectIndex, int fromIndex, int whereIndex, int usingIndex) ParseQueryTokens(string[] tokens)
    {
        int selectIdx = -1;
        int fromIdx = -1;
        int whereIdx = -1;
        int usingIdx = -1;

        for (int i = 0; i < tokens.Length; i++)
        {
            if (tokens[i].Equals("SELECT", StringComparison.OrdinalIgnoreCase)) selectIdx = i;
            else if (tokens[i].Equals("FROM", StringComparison.OrdinalIgnoreCase)) fromIdx = i;
            else if (tokens[i].Equals("WHERE", StringComparison.OrdinalIgnoreCase)) whereIdx = i;
            else if (tokens[i].Equals("USING", StringComparison.OrdinalIgnoreCase)) usingIdx = i;
        }
        return (selectIdx, fromIdx, whereIdx, usingIdx);
    }
}