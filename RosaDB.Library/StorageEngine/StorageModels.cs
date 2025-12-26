namespace RosaDB.Library.StorageEngine;

public readonly record struct TableInstanceIdentifier(string CellName, string TableName, string InstanceHash);
public readonly record struct SegmentMetadata(int CurrentSegmentNumber, long CurrentSegmentSize);
public readonly record struct LogLocation(int SegmentNumber, long Offset);