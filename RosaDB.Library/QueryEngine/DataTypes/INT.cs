using RosaDB.Library.Core;

namespace RosaDB.Library.QueryEngine.DataTypes;

public class INT : DataType
{
    public int Value;
    
    public static Result<DataType> Create(int value)
    {
        return new INT()
        {
            Value = value
        };
    }
    
    public override byte[] GetContent() => BitConverter.GetBytes(Value);
}