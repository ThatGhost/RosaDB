using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine;

public readonly record struct TableInstanceIdentifier(string ContextName, string TableName, string InstanceHash);
public readonly record struct SegmentMetadata(int CurrentSegmentNumber, long CurrentSegmentSize);
public readonly record struct LogLocation(int SegmentNumber, long Offset);
public readonly record struct ColumnValue(Column Column, object Value);