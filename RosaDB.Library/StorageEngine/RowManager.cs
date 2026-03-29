using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;
using RosaDB.Library.StorageEngine.Serializers;
using System.IO.Abstractions;

namespace RosaDB.Library.StorageEngine;

public class RowManager(
    SessionState session, 
    ILogWriter logWriter, 
    ILogReader logReader,
    IFileSystem fileSystem,
    IIndexManager indexManager,
    IFolderManager folderManager
    ) : IRowManager
{
    public async Task<Result<Row>> GetRow(string moduleName, string tableName, string moduleInstance, long logId)
    {
        var (pathToComposite, compositeLogIndexPath, tableInstanceHashIndexFilePath) = GetRowContext(moduleName, tableName, moduleInstance);

        var logLocation = indexManager.GetStruct<long, LogLocation>(compositeLogIndexPath, logId);
        if (logLocation is null) return new Error(ErrorPrefixes.DataError, "Row does not exist");

        var logResult = await logReader.FindLog((LogLocation)logLocation);
        if (logResult.IsFailure) return logResult.Error;

        return RowSerializer.Deserialize(logResult.Value.TupleData, session.CurrentDatabase?.GetModule(moduleName)?.GetTable(tableName)?.Columns.ToArray() ?? []);
    }

    // TODO Transactions not working yet
    public async Task<Result> InsertRow(Row row, string moduleName, string tableName, string moduleInstance)
    {
        var validationResult = ValidateRowToTable(row, moduleName, tableName);
        if (validationResult.IsFailure) return validationResult;
        
        var (pathToComposite, compositeLogIndexPath, tableInstanceHashIndexFilePath) = GetRowContext(moduleName, tableName, moduleInstance);

        if (indexManager.Get(tableInstanceHashIndexFilePath, row.InstanceHash).Length != 0) return new Error(ErrorPrefixes.DataError, "Index or Primary key is not unique");
        
        long newLogId = indexManager.GetNextKey<long>(compositeLogIndexPath);
        indexManager.Insert(compositeLogIndexPath, newLogId, []); // reserve logId
        string logFilePath = GetCurrentDataPath(pathToComposite, moduleInstance);
        
        logWriter.Insert(logFilePath, row, newLogId);
        var commitResult = await logWriter.Commit();
        if (commitResult.IsFailure || !commitResult.Value.TryGetValue(newLogId, out LogLocation logLocation))
        {
            indexManager.Delete(compositeLogIndexPath, newLogId);
            return commitResult.IsFailure ? commitResult.Error : new Error(ErrorPrefixes.FileError, "Row could not be inserted");
        }

        indexManager.Update(compositeLogIndexPath, newLogId, logLocation);
        if(row.InstanceHash != "") indexManager.Insert(tableInstanceHashIndexFilePath, row.InstanceHash, logLocation);
        
        return Result.Success();
    }

    public async Task<Result> UpdateRow(Row row, string moduleName, string tableName, string moduleInstance, long logId)
    {
        var validationResult = ValidateRowToTable(row, moduleName, tableName);
        if (validationResult.IsFailure) return validationResult;

        var (pathToComposite, compositeLogIndexPath, tableInstanceHashIndexFilePath) = GetRowContext(moduleName, tableName, moduleInstance);
        if (indexManager.Get(tableInstanceHashIndexFilePath, row.InstanceHash).Length == 0) return new Error(ErrorPrefixes.DataError, "Row does not exist");

        string logFilePath = GetCurrentDataPath(pathToComposite, moduleInstance);
        logWriter.Update(logFilePath, row, logId);

        var commitResult = await logWriter.Commit();
        if (commitResult.IsFailure || !commitResult.Value.TryGetValue(logId, out LogLocation logLocation))
        {
            return commitResult.IsFailure ? commitResult.Error : new Error(ErrorPrefixes.FileError, "Row could not be updated");
        }

        indexManager.Update(compositeLogIndexPath, logId, logLocation);
        if (row.InstanceHash != "") indexManager.Update(tableInstanceHashIndexFilePath, row.InstanceHash, logLocation);

        return Result.Success();
    }

    public async Task<Result> DeleteRow(string moduleName, string tableName, string moduleInstance, long logId)
    {
        var (pathToComposite, compositeLogIndexPath, tableInstanceHashIndexFilePath) = GetRowContext(moduleName, tableName, moduleInstance);
        string logFilePath = GetCurrentDataPath(pathToComposite, moduleInstance);

        var rowResult = await GetRow(moduleName, tableName, moduleInstance, logId);
        if (rowResult.IsFailure) return rowResult.Error;

        logWriter.Delete(logFilePath, logId);
        var commitResult = await logWriter.Commit();
        if (commitResult.IsFailure || !commitResult.Value.TryGetValue(logId, out LogLocation newLogLocation))
        {
            return commitResult.IsFailure ? commitResult.Error : new Error(ErrorPrefixes.FileError, "Row could not be deleted");
        }

        indexManager.Delete(compositeLogIndexPath, logId);
        indexManager.Delete(tableInstanceHashIndexFilePath, rowResult.Value.InstanceHash);

        return Result.Success();
    }

    public Task<Result> Commit()
    {
        throw new NotImplementedException();
    }

    public Task<Result> Rollback()
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Retrieves the file system paths related to a specific table instance within a module, based on the provided
    /// module name, table name, and instance hash.
    /// </summary>
    /// <param name="moduleName">The name of the module containing the table. Cannot be null or empty.</param>
    /// <param name="tableName">The name of the table for which to retrieve context paths. Cannot be null or empty.</param>
    /// <param name="moduleInstance">The unique hash identifying the specific table instance. Cannot be null or empty.</param>
    /// <returns>A tuple containing the path to the composite directory, the path to the composite log index file, and the path
    /// to the table instance hash index file.</returns>
    private (string pathToComposite, string compositeLogIndexPath, string tableInstanceHashIndexFilePath) GetRowContext(string moduleName, string tableName, string moduleInstance)
    {
        string compositeName = $"{moduleName}.{tableName}";
        string pathToComposite = GetTablePath(compositeName, moduleInstance);
        
        string compositeLogIndexPath = fileSystem.Path.Combine(pathToComposite, "log.idx");
        string tableInstanceHashIndexFilePath = fileSystem.Path.Combine(pathToComposite, $"{moduleInstance}_hash.idx");
        
        return (pathToComposite, compositeLogIndexPath, tableInstanceHashIndexFilePath);
    }
    
    private string GetTablePath(string compositeName, string moduleInstance)
        => fileSystem.Path.Combine(folderManager.BasePath, compositeName, moduleInstance[..3]);
    
    private IEnumerable<string> GetAllDatFiles(string path, string moduleInstance)
        => fileSystem.Directory.EnumerateFiles(path, $"{moduleInstance}_*.dat");

    /// <summary>
    /// File with where the new row should be inserted. If the current file is larger than 1mb, a new file will be created with an incremented number at the end of the file name.
    /// </summary>
    private string GetCurrentDataPath(string tablePath, string moduleInstance)
    {
        // file = <moduleInstance>_<number>.dat
        var highestFile = GetAllDatFiles(tablePath, moduleInstance)
            .Select(p => (path: p, num: int.Parse(fileSystem.Path.GetFileNameWithoutExtension(p))))
            .OrderByDescending(f => f.num)
            .FirstOrDefault();

        if (highestFile == default)
        {
            string newPath = fileSystem.Path.Combine(tablePath, $"{moduleInstance}_0.dat");
            return newPath;
        }
        
        var fileInfo = fileSystem.FileInfo.New(highestFile.path);
        return fileInfo is { Exists: true, Length: < 1_000_000 } ? 
            highestFile.path : 
            fileSystem.Path.Combine(tablePath, $"{moduleInstance}_{highestFile.num + 1}.dat");
    }

    private Result ValidateRowToTable(Row row, string moduleName, string tableName)
    {
        var columns = session.CurrentDatabase?.GetModule(moduleName)?.GetTable(tableName)?.Columns;
        if (columns is null || columns.Count == 0) return new Error(ErrorPrefixes.StateError, "Table and/or module not part of database");

        foreach (var column in columns)
        {
            object? value = row.GetValue(column.Name);
            if (value is null)
            {
                if (column.IsIndex) return new Error(ErrorPrefixes.DataError, $"Index column {column.Name} cannot be null");
                if(!column.IsNullable) return new Error(ErrorPrefixes.DataError, "Trying to enter NULL into a non-nullable column");
                continue;
            }
            if (!IsValueCorrectType(value, column.DataType)) return new Error(ErrorPrefixes.DataError, $"value {value} does not match column description: {column.DataType}");
        }
        
        return Result.Success();
    }

    private bool IsValueCorrectType(object value, DataType dataType)
    {
        return dataType switch
        {
            DataType.BIGINT => value is long,
            DataType.INTEGER  => value is int,
            DataType.INT => value is int,
            DataType.BOOLEAN => value is bool,
            DataType.CHAR => value is char,
            DataType.CHARACTER => value is char,
            DataType.DATETIME => value is DateTime,
            DataType.DECIMAL => value is decimal,
            DataType.FLOAT => value is float,
            DataType.LONG => value is long,
            DataType.NUMBER => value is decimal,
            DataType.NUMERIC => value is int or long or float or decimal or short,
            DataType.SMALLINT => value is short,
            DataType.TEXT => value is string,
            DataType.VARCHAR => value is string,
            _ => false
        };
    }
}