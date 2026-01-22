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

    public static readonly string DatabaseName = "database";
    public static void SetupContext(Mock<SessionState> sessionState)
    {
        sessionState.Setup(s => s.CurrentDatabase).Returns(Database.Create(DatabaseName).Value);
    }
}