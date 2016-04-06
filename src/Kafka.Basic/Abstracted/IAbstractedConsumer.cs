using System;
using System.Threading.Tasks;

namespace Kafka.Basic.Abstracted
{
    public interface IAbstractedConsumer : IDisposable
    {
        void Shutdown();
    }

    public interface IAbstractedConsumer<out T> : IAbstractedConsumer
    {
        void Start(
            Action<T> dataSubscriber,
            Action<Exception> errorSubscriber = null,
            Action closeAction = null);
        Task StartAsync(
            Action<T> dataSubscriber,
            Action<Exception> errorSubscriber = null,
            Action closeAction = null);
    }
}