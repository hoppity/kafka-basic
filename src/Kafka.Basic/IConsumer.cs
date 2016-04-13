using System;

namespace Kafka.Basic
{
    public interface IConsumer<out T> : IDisposable
    {
        void Start(
            Action<T> dataSubscriber,
            Action<Exception> errorSubscriber = null,
            Action closeAction = null);
        void Shutdown();
    }
}