using System;

namespace Kafka.Basic.Abstracted
{
    public interface IAbstractedConsumer<out T> : IDisposable
    {
        void Start(
            Action<T> dataSubscriber,
            Action<Exception> errorSubscriber = null,
            Action closeAction = null);
        void Shutdown();
    }
}