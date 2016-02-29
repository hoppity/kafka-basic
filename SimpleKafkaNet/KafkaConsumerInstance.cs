using System.Collections.Generic;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;

namespace SimpleKafkaNet
{
    public interface IKafkaConsumerInstance
    {

    }

    public class KafkaConsumerInstance : IKafkaConsumerInstance
    {
        private readonly ConsumerConfiguration _consumerConfig;
        private readonly ZookeeperConsumerConnector _balancedConsumer;

        public KafkaConsumerInstance(ConsumerConfiguration consumerConfig)
        {
            _consumerConfig = consumerConfig;

            _balancedConsumer = new ZookeeperConsumerConnector(_consumerConfig, true);
        }

        public IEnumerable<IKafkaMessageStream> Subscribe(IEnumerable<Subscription> subscriptions)
        {
            
        }
    }
}
