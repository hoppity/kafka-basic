using System;
using Kafka.Basic.Abstracted;

namespace Kafka.Basic.Auto
{
    public interface IConsumerTupleFactory
    {
        Tuple<IConsumer, IAbstractedConsumer> TryBuildFor(IConsumer consumer);
    }

    public class ConsumerTupleFactory : IConsumerTupleFactory
    {
        private readonly IKafkaClient _client;

        public ConsumerTupleFactory(IKafkaClient client)
        {
            _client = client;
        }

        public Tuple<IConsumer, IAbstractedConsumer> TryBuildFor(IConsumer consumer)
        {
            var balancedConsumer = consumer as IBalancedConsumer;
            if (balancedConsumer != null) return Balanced(balancedConsumer);

            var batchedConsumer = consumer as IBatchedConsumer;
            if (batchedConsumer != null) return Batched(batchedConsumer);

            return null;
        }

        private Tuple<IConsumer, IAbstractedConsumer> Balanced(IBalancedConsumer consumer)
        {
            var kafkaConsumer = new Abstracted.BalancedConsumer(_client, consumer.Group, consumer.Topic);
            kafkaConsumer.Start(consumer.Receive);
            return new Tuple<IConsumer, IAbstractedConsumer>(consumer, kafkaConsumer);
        }

        private Tuple<IConsumer, IAbstractedConsumer> Batched(IBatchedConsumer consumer)
        {
            var kafkaConsumer = new BatchedConsumer(_client, consumer.Group, consumer.Topic);
            kafkaConsumer.Start(consumer.Receive);
            return new Tuple<IConsumer, IAbstractedConsumer>(consumer, kafkaConsumer);
        }
    }
}
