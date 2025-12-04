using RosaDB.Library.Core;

namespace RosaDB.Library.QueryEngine;

public abstract class DataType
{
    public abstract byte[] GetContent();
}