using RosaDB.Library.Core;

namespace RosaDB.Library.Query.TokenParsers
{
    public static class TokensToParenthesizedList
    {
        public static Result<string[]> ParseParenthesizedList(string[] tokens, int startIndex, out int endIndex)
        {
            endIndex = -1;
            if (startIndex >= tokens.Length) return new Error(ErrorPrefixes.QueryParsingError, "Unexpected end of query.");

            var openParenIndex = Array.IndexOf(tokens, "(", startIndex);
            if (openParenIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing opening parenthesis.");

            var closeParenIndex = Array.IndexOf(tokens, ")", openParenIndex);
            if (closeParenIndex == -1) return new Error(ErrorPrefixes.QueryParsingError, "Missing closing parenthesis.");

            endIndex = closeParenIndex;
            var listTokens = tokens[(openParenIndex + 1)..closeParenIndex].Where(t => t != ",").ToArray();
            return listTokens;
        }
    }
}
