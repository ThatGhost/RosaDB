using Moq;
using RosaDB.Library.Core;
using RosaDB.Library.Models;
using RosaDB.Library.Server;

namespace RosaDB.Library.Tests;

public static class Utils
{
    public static void AssertSuccess(Result result)
    {
        Assert.That(result, Is.Not.Null);
        Assert.That(result.IsSuccess, Is.True);
        Assert.That(result.Error, Is.Null);
    }
    
    public static void AssertSuccess<T>(Result<T> result)
    {
        Assert.That(result, Is.Not.Null);
        Assert.That(result.IsSuccess, Is.True);
        Assert.That(result.Error, Is.Null);
        Assert.That(result.Value, Is.Not.Null);
    }

    public static void AssertFailure(Result result, Error error)
    {
        Assert.That(result, Is.Not.Null);
        Assert.That(result.IsSuccess, Is.False);
        Assert.That(error, Is.Not.Null);
        Assert.That(result.Error, Is.Not.Null);
        Assert.That(result.Error, Is.EqualTo(error));
    }
    
    public static void AssertFailure<T>(Result<T> result, Error error)
    {
        Assert.That(result, Is.Not.Null);
        Assert.That(result.IsSuccess, Is.False);
        Assert.That(error, Is.Not.Null);
        Assert.That(result.Error, Is.Not.Null);
        Assert.That(result.Error, Is.EqualTo(error));
        Assert.That(result.Value, Is.Null);
    }

    public static readonly string DatabaseName = "database";
    public static void SetupSessionState(Mock<SessionState> sessionState)
    {
        sessionState.Setup(s => s.CurrentDatabase).Returns(Database.Create(DatabaseName).Value);
    }
}