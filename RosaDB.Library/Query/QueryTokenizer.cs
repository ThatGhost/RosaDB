using System;
using System.Collections.Generic;
using System.Text;
using RosaDB.Library.Core;

namespace RosaDB.Library.Query
{
    public class QueryTokenizer
    {
        public Result<List<string[]>> Tokenize(string query)
        {
            if (string.IsNullOrWhiteSpace(query))  return new List<string[]>();

            var queries = query.Split(';', StringSplitOptions.RemoveEmptyEntries);
            var tokenizedQueries = new List<string[]>();

            foreach (var singleQuery in queries)
            {
                if (string.IsNullOrWhiteSpace(singleQuery)) continue;

                var tokensResult = _TokenizeSingleQuery(singleQuery);
                if (!tokensResult.TryGetValue(out var tokens)) return tokensResult.Error;
                tokenizedQueries.Add(tokens);
            }
            return tokenizedQueries;
        }

        private Result<string[]> _TokenizeSingleQuery(string query)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(query)) return Array.Empty<string>();

                List<string> tokens = new();
                ReadOnlySpan<char> span = query.AsSpan();
                int n = span.Length;
                int tokenStart = 0;
                bool inToken = false;

                for (int i = 0; i < n; i++)
                {
                    char c = span[i];

                    // Handle Quotes
                    if (c is '"' or '\'')
                    {
                        if (inToken)
                        {
                            tokens.Add(span.Slice(tokenStart, i - tokenStart).Trim().ToString());
                            inToken = false;
                        }

                        char quote = c;
                        int literalStart = i + 1;
                        i++;
                        while (i < n && span[i] != quote) i++;

                        tokens.Add(i < n
                            ? span.Slice(literalStart, i - literalStart).ToString()
                            : span.Slice(literalStart, n - literalStart).ToString());
                        continue;
                    }

                    // Handle Separators
                    bool isSeparator = c == '(' || c == ')' || c == ',' || c == ';' || char.IsWhiteSpace(c);

                    if (isSeparator)
                    {
                        if (inToken)
                        {
                            tokens.Add(span.Slice(tokenStart, i - tokenStart).Trim().ToString());
                            inToken = false;
                        }

                        if (!char.IsWhiteSpace(c))
                        {
                            tokens.Add(c.ToString());
                        }
                    }
                    else
                    {
                        if (!inToken)
                        {
                            tokenStart = i;
                            inToken = true;
                        }
                    }
                }

                if (inToken)
                {
                    tokens.Add(span.Slice(tokenStart, n - tokenStart).Trim().ToString());
                }

                return tokens.ToArray();
            }
            catch { return new CriticalError(); }
        }
    }
}