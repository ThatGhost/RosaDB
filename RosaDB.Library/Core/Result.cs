using System.Diagnostics.CodeAnalysis;

namespace RosaDB.Library.Core;

public record Error(ErrorPrefixes Prefix, string Message);
public record DatabaseNotSetError() : Error(ErrorPrefixes.StateError, "Database not set");
public record CriticalError() : Error(ErrorPrefixes.CriticalError, "Something went wrong");
public sealed class Result<T>
{
    [AllowNull]
    private readonly T _value;

    private Result(T value)
    {
        _value = value;
        IsSuccess = true;
    }

    private Result(Error error)
    {
        IsSuccess = false;
        Error = error;
    }

    [MemberNotNullWhen(false, nameof(Error))]
    [MemberNotNullWhen(true, nameof(Value))]
    public bool IsSuccess { get; }

    [MemberNotNullWhen(true, nameof(Error))]
    [MemberNotNullWhen(false, nameof(Value))]
    public bool IsFailure => !IsSuccess;

    public Error? Error { get; }

    public T? Value => _value!;

    public static implicit operator Result<T>(T value) => new(value);
    public static implicit operator Result<T>(Error error) => new(error);

    public static Result<T> Success(T value) => new(value);
    public static Result<T> Failure(Error error) => new(error);

    public TResult Match<TResult>(Func<T, TResult> onSuccess, Func<Error, TResult> onFailure)
        => IsSuccess ? onSuccess(Value!) : onFailure(Error);

    public async Task<TResult> MatchAsync<TResult>(
        Func<T, Task<TResult>> onSuccess,
        Func<Error, Task<TResult>> onFailure)
    {
        return IsSuccess
            ? await onSuccess(Value)
            : await onFailure(Error);
    }

    [MemberNotNullWhen(false, nameof(Error))]
    [MemberNotNullWhen(true, nameof(Value))]
    public bool TryGetValue([NotNullWhen(true)] out T? value)
    {
        value = _value;
        return IsSuccess;
    }
}

public sealed class Result
{
    private Result(Error error)
    {
        IsSuccess = false;
        Error = error;
    }

    private Result()
    {
        IsSuccess = true;
    }

    [MemberNotNullWhen(false, nameof(Error))]
    public bool IsSuccess { get; }
    [MemberNotNullWhen(true, nameof(Error))]
    public bool IsFailure => !IsSuccess;

    public Error? Error { get; }

    public static implicit operator Result(Error error) => new(error);

    public static Result Success() => new();
    public static Result Failure(Error error) => new(error);

    public TResult Match<TResult>(Func<TResult> onSuccess, Func<Error, TResult> onFailure)
        => IsSuccess ? onSuccess() : onFailure(Error);

    public async Task<TResult> MatchAsync<TResult>(
        Func<Task<TResult>> onSuccess,
        Func<Error, Task<TResult>> onFailure)
    {
        return IsSuccess
            ? await onSuccess()
            : await onFailure(Error);
    }
}