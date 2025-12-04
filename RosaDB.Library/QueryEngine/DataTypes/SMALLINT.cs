using System.Text;
using RosaDB.Library.Core;

namespace RosaDB.Library.QueryEngine.DataTypes;

public class SMALLINT : DataType
{
    private short Value;
    
    public static Result<DataType> Create(short value)
    {
        return new SMALLINT()
        {
            Value = value
        };
    }
    
    public override byte[] GetContent() => BitConverter.GetBytes(Value);
}