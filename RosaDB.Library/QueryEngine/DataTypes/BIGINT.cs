using System.Text;
using RosaDB.Library.Core;

namespace RosaDB.Library.QueryEngine.DataTypes;

public class BIGINT : DataType
{
    private long Value;
    
    public static Result<DataType> Create(long value)
    {
        return new BIGINT()
        {
            Value = value
        };
    }

    public override byte[] GetContent() => BitConverter.GetBytes(Value);
}