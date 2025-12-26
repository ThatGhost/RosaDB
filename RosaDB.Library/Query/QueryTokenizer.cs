using System;
using System.Collections.Generic;
using System.Text;
using RosaDB.Library.Core;

namespace RosaDB.Library.Query
{
    public class QueryTokenizer
    {
        public Result<string[]> TokenizeQuery(string query)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(query)) return Array.Empty<string>();

                List<string> tokens = new();
                var currentToken = new StringBuilder();
                bool inLiteral = false;
                char literalChar = '\'';

                var PushToken = () =>
                {
                    if (currentToken.Length > 0)
                    {
                        tokens.Add(currentToken.ToString());
                        currentToken.Clear();
                    }
                };

                foreach (char c in query)
                {
                    if (inLiteral)
                    {
                        if (c == literalChar)
                        {
                            tokens.Add(currentToken.ToString());
                            currentToken.Clear();
                            inLiteral = false;
                        }
                        else currentToken.Append(c);
                        continue;
                    }

                    if (c == '(' || c == ')' || c == ',' || c == ';' || char.IsWhiteSpace(c))
                    {
                        PushToken();
                        if(!char.IsWhiteSpace(c)) tokens.Add(c.ToString());
                    }
                    else if (c == '"' || c == '\'')
                    {
                        PushToken(); 
                        inLiteral = true;
                        literalChar = c;
                    }
                    else currentToken.Append(c);
                }

                PushToken();
                return tokens.Select(t => t.Trim()).ToArray();
            }
            catch { return new CriticalError(); }
        }
    }
}