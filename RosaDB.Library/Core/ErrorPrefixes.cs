namespace RosaDB.Library.Core;

public record ErrorPrefixes(string Prefix)
{
    public static readonly ErrorPrefixes QueryParsingError = new("Query parsing error: ");
    public static readonly ErrorPrefixes DataError = new("Data error: ");
    public static readonly ErrorPrefixes FileError = new("File error: ");
    public static readonly ErrorPrefixes StateError = new("State error: ");
    public static readonly ErrorPrefixes CriticalError = new("Critical error: ");
}