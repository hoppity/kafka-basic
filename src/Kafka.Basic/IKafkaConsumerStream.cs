using System;
using System.Collections.Generic;

namespace Kafka.Basic
{
    public interface IKafkaStream : IDisposable
    {
        void Block();
        void Shutdown();
        void Pause();
        void Resume();
    }

    public interface IKafkaConsumerStream : IKafkaStream
    {
        IKafkaConsumerStream Data(Action<ConsumedMessage> action);
        IKafkaConsumerStream Error(Action<Exception> action);
        IKafkaConsumerStream Close(Action action);
        IKafkaConsumerStream Start();
    }

    public interface IKafkaBatchedConsumerStream : IKafkaStream
    {
        IKafkaBatchedConsumerStream Data(Action<IEnumerable<ConsumedMessage>> action);
        IKafkaBatchedConsumerStream Error(Action<Exception> action);
        IKafkaBatchedConsumerStream Close(Action action);
        IKafkaBatchedConsumerStream Start();
    }
}