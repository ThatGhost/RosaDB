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

                foreach (char c in query)
                {
                    if (c == '(' || c == ')')
                    {
                        if (currentToken.Length > 0)
                        {
                            tokens.Add(currentToken.ToString());
                            currentToken.Clear();
                        }
                        tokens.Add(c.ToString());
                    }
                    else if (c == ';')
                    {
                        if (currentToken.Length > 0)
                        {
                            tokens.Add(currentToken.ToString());
                            currentToken.Clear();
                        }
                        tokens.Add(c.ToString());
                    }
                    else if (c == ',')
                    {
                        if (currentToken.Length > 0)
                        {
                            tokens.Add(currentToken.ToString());
                            currentToken.Clear();
                        }
                        tokens.Add(c.ToString());
                    }
                    else if (char.IsWhiteSpace(c))
                    {
                        if (currentToken.Length > 0)
                        {
                            tokens.Add(currentToken.ToString());
                            currentToken.Clear();
                        }
                    }
                    else currentToken.Append(c);
                }

                if (currentToken.Length > 0) tokens.Add(currentToken.ToString());

                return tokens.Select(t => t.Trim()).ToArray();
            }
            catch { return new CriticalError(); }
        }
    }
}