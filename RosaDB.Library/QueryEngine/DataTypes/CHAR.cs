using System.Text;
using RosaDB.Library.Core;

namespace RosaDB.Library.QueryEngine.DataTypes;

public class CHAR : DataType
{
    private byte[] Content = [];

    public static Result<DataType> Create(short size, string content)
    {
        byte[] c = Encoding.UTF8.GetBytes(content);
        if (c.Length != size) return new Error(ErrorPrefixes.DatatypeError, "Content is not the same size as set size");
        
        return new CHAR()
        {
            Content = c
        };
    }

    public override byte[] GetContent() => Content;
}