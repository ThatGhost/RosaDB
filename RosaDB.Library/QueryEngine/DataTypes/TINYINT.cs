using RosaDB.Library.Core;

namespace RosaDB.Library.QueryEngine.DataTypes;

public class TINYINT : DataType
{
    private byte Value;
    
    public static Result<DataType> Create(byte value)
    {
        return new TINYINT()
        {
            Value = value
        };
    }

    public override byte[] GetContent() => [Value];
}