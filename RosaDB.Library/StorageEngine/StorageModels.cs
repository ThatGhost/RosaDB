using RosaDB.Library.Models;

namespace RosaDB.Library.StorageEngine;

public readonly record struct TableInstanceIdentifier(string module, string TableName, string InstanceHash);
public readonly record struct LogLocation(string Path, long Offset, long logId);
