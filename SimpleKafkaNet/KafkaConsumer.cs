using System.Collections.Generic;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;

namespace SimpleKafkaNet
{
    public interface IKafkaConsumer
    {
        IKafkaConsumerInstance Join(KafkaConsumerOptions options = null);
    }

    public class KafkaConsumer : IKafkaConsumer
    {
        private readonly ZooKeeperConfiguration _zkConfig;
        private readonly string _groupName;

        public KafkaConsumer(ZooKeeperConfiguration zkConfig, string groupName)
        {
            _zkConfig = zkConfig;
            _groupName = groupName;
        }

        public IKafkaConsumerInstance Join(KafkaConsumerOptions options = null)
        {
            options = options ?? new KafkaConsumerOptions();
            var consumerConfig = new ConsumerConfiguration
            {
                AutoCommit = options.AutoCommitEnabled,
                GroupId = _groupName,
                AutoOffsetReset = options.AutoOffsetReset,
                NumberOfTries = 20,
                ZooKeeper = _zkConfig
            };
            return new KafkaConsumerInstance(consumerConfig);
        }
    }
}