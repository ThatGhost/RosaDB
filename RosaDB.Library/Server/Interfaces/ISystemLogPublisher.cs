
namespace RosaDB.Library.Server.Interfaces
{
    public interface ISystemLogPublisher
    {
        void Publish(LogLevel level, string message);
    }
}
