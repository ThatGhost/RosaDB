namespace RosaDB.Library.StorageEngine;

public readonly record struct TableInstanceIdentifier(string CellName, string TableName, string InstanceHash, int Version = 1);
public readonly record struct SegmentMetadata(int CurrentSegmentNumber, long CurrentSegmentSize);
public readonly record struct SparseIndexEntry(long Key, long Offset, int Version = 1);
public readonly record struct IndexHeader(string CellName, string TableName, string InstanceHash, int SegmentNumber, int Version = 1);
