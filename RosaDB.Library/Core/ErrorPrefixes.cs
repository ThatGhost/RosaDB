namespace RosaDB.Library.Core;

public record ErrorPrefixes(string Prefix)
{
    public static readonly ErrorPrefixes QueryParsingError = new ErrorPrefixes("Query parsing error: ");
    public static readonly ErrorPrefixes QueryExecutionError = new ErrorPrefixes("Query execution error: ");
    public static readonly ErrorPrefixes DatatypeError = new ErrorPrefixes("Datatype error: ");
    public static readonly ErrorPrefixes FileError = new ErrorPrefixes("File error: ");
    public static readonly ErrorPrefixes StateError = new ErrorPrefixes("State error: ");
    public static readonly ErrorPrefixes CriticalError = new ErrorPrefixes("Critical error: ");
}