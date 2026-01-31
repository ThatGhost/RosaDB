using System.IO.Abstractions;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;
using RosaDB.Library.StorageEngine.Interfaces;

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
    // TODO Transactions not working yet
    public async Task<Result> InsertRow(Row row, string moduleName, string tableName, string instanceHash)
    {
        var validationResult = ValidateRowToTable(row, moduleName, tableName);
        if (validationResult.IsFailure) return validationResult;
        
        var (pathToComposite, compositeLogIndexPath, tableInstanceHashIndexFilePath) = GetRowContext(moduleName, tableName, instanceHash);

        if (indexManager.Get(tableInstanceHashIndexFilePath, row.InstanceHash).Length != 0) return new Error(ErrorPrefixes.DataError, "Index or Primary key is not unique");
        
        long newLogId = indexManager.GetNextKey<long>(compositeLogIndexPath);
        indexManager.Insert(compositeLogIndexPath, newLogId, []); // reserve logId
        string logFilePath = GetCurrentDataPath(pathToComposite, instanceHash);
        
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

    public Task<Result> UpdateRow(Row row, string moduleName, string tableName, string instanceHash, long logId)
    {
        throw new NotImplementedException();
    }

    public Task<Result> DeleteRow(string moduleName, string tableName, string instanceHash, long logId)
    {
        throw new NotImplementedException();
    }

    public Task<Result> Commit()
    {
        throw new NotImplementedException();
    }

    public Task<Result> Rollback()
    {
        throw new NotImplementedException();
    }

    private (string pathToComposite, string compositeLogIndexPath, string tableInstanceHashIndexFilePath) GetRowContext(string moduleName, string tableName, string instanceHash)
    {
        string compositeName = $"{moduleName}.{tableName}";
        string pathToComposite = GetTablePath(compositeName, instanceHash);
        
        string compositeLogIndexPath = fileSystem.Path.Combine(pathToComposite, "log.idx");
        string tableInstanceHashIndexFilePath = fileSystem.Path.Combine(pathToComposite, $"{instanceHash}_hash.idx");
        
        return (pathToComposite, compositeLogIndexPath, tableInstanceHashIndexFilePath);
    }
    
    private string GetTablePath(string compositeName, string instanceHash)
        => fileSystem.Path.Combine(folderManager.BasePath, compositeName, instanceHash[..3]);
    
    private IEnumerable<string> GetAllDatFiles(string path, string instanceHash)
        => fileSystem.Directory.EnumerateFiles(path, $"{instanceHash}_*.dat");

    private string GetCurrentDataPath(string tablePath, string instanceHash)
    {
        // file = <instanceHash>_<number>.dat
        var highestFile = GetAllDatFiles(tablePath, instanceHash)
            .Select(p => (path: p, num: int.Parse(fileSystem.Path.GetFileNameWithoutExtension(p))))
            .OrderByDescending(f => f.num)
            .FirstOrDefault();

        if (highestFile == default)
        {
            string newPath = fileSystem.Path.Combine(tablePath, $"{instanceHash}_0.dat");
            return newPath;
        }
        
        var fileInfo = fileSystem.FileInfo.New(highestFile.path);
        return fileInfo is { Exists: true, Length: < 1_000_000 } ? 
            highestFile.path : 
            fileSystem.Path.Combine(tablePath, $"{instanceHash}_{highestFile.num + 1}.dat");
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