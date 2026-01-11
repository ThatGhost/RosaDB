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

    public Result(Error error)
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

    public Result<U> Then<U>(Func<T, Result<U>> func)
    {
        return IsSuccess ? func(Value!) : new Result<U>(Error!);
    }

    public async Task<Result<U>> ThenAsync<U>(Func<T, Task<Result<U>>> func)
    {
        return IsSuccess ? await func(Value!) : new Result<U>(Error!);
    }

    public async Task<Result> ThenAsync(Func<T, Task<Result>> func)
    {
        return IsSuccess ? await func(Value!) : Result.Failure(Error!);
    }
    
    public Result Finally(Func<T,Result> func)
    {
        return IsSuccess ? func(Value!) : Error!;
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

    public Result<T> Then<T>(Func<Result<T>> func)
    {
        return IsSuccess ? func() : new Result<T>(Error!);
    }
    
    public Result Finally(Func<Result> func)
    {
        return IsSuccess ? func() : new Result(Error!);
    }

    public async Task<Result<T>> ThenAsync<T>(Func<Task<Result<T>>> func)
    {
        return IsSuccess ? await func() : new Result<T>(Error!);
    }
}

public static class ResultExtensions
{
    public static async Task<Result<U>> Then<T, U>(this Task<Result<T>> resultTask, Func<T, Result<U>> func)
    {
        var result = await resultTask;
        return result.Then(func);
    }

    public static async Task<Result<U>> ThenAsync<T, U>(this Task<Result<T>> resultTask, Func<T, Task<Result<U>>> func)
    {
        var result = await resultTask;
        return await result.ThenAsync(func);
    }

    public static async Task<Result> ThenAsync<T>(this Task<Result<T>> resultTask, Func<T, Task<Result>> func)
    {
        var result = await resultTask;
        return await result.ThenAsync(func);
    }

    public static async Task<TResult> MatchAsync<TResult>(
        this Task<Result> resultTask,
        Func<Task<TResult>> onSuccess,
        Func<Error, Task<TResult>> onFailure)
    {
        var result = await resultTask;
        return await result.MatchAsync(onSuccess, onFailure);
    }
}