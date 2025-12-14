using System.Diagnostics.CodeAnalysis;

namespace RosaDB.Library.Core;

public record Error(ErrorPrefixes Prefix, string Message);
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

    public bool IsSuccess { get; }
    public bool IsFailure { get => !IsSuccess; }
    public Error? Error { get; }

    public T Value => _value!;

    public static implicit operator Result<T>(T value) => new(value);
    public static implicit operator Result<T>(Error error) => new(error);

    public static Result<T> Success(T value) => new(value);
    public static Result<T> Failure(Error error) => new(error);

    public TResult Match<TResult>(Func<T, TResult> onSuccess, Func<Error, TResult> onFailure)
        => IsSuccess ? onSuccess(Value!) : onFailure(Error!);

    public async Task<TResult> MatchAsync<TResult>(
        Func<T, Task<TResult>> onSuccess,
        Func<Error, Task<TResult>> onFailure)
    {
        return IsSuccess
            ? await onSuccess(Value)
            : await onFailure(Error!);
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

    public bool IsSuccess { get; }
    public bool IsFailure { get => !IsSuccess; }
    public Error? Error { get; }

    public static implicit operator Result(Error error) => new(error);

    public static Result Success() => new();
    public static Result Failure(Error error) => new(error);

    public TResult Match<TResult>(Func<TResult> onSuccess, Func<Error, TResult> onFailure)
        => IsSuccess ? onSuccess() : onFailure(Error!);

    public async Task<TResult> MatchAsync<TResult>(
        Func<Task<TResult>> onSuccess,
        Func<Error, Task<TResult>> onFailure)
    {
        return IsSuccess
            ? await onSuccess()
            : await onFailure(Error!);
    }
}