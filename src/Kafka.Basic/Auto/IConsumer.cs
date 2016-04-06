using System.Collections.Generic;

namespace Kafka.Basic.Auto
{
    public interface IConsumer
    {
        string Group { get; }
        string Topic { get; }
    }

    public interface IBalancedConsumer : IConsumer
    {
        void Receive(ConsumedMessage message);
    }

    public interface IBatchedConsumer : IConsumer
    {
        void Receive(IEnumerable<ConsumedMessage> messages);
    }
}
