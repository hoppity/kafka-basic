using System;
using Kafka.Client.Cfg;
using Kafka.Client.ZooKeeperIntegration;

namespace SimpleKafka
{
    public interface IKafkaClient : IDisposable
    {
        IKafkaTopic Topic(string name);
    }

    public class KafkaClient : IKafkaClient
    {
        private readonly ZooKeeperClient _zkClient;

        public KafkaClient(string zkConnect)
        {
            var kafkaConfig = new KafkaSimpleManagerConfiguration()
            {
                Zookeeper = zkConnect,
                MaxMessageSize = SyncProducerConfiguration.DefaultMaxMessageSize,
                PartitionerClass = ProducerConfiguration.DefaultPartitioner
            };
            kafkaConfig.Verify();

            _zkClient = new ZooKeeperClient(
                zkConnect,
                ZooKeeperConfiguration.DefaultSessionTimeout,
                ZooKeeperStringSerializer.Serializer
                );
            _zkClient.Connect();
        }

        public IKafkaTopic Topic(string name)
        {
            return new KafkaTopic(_zkClient, name);
        }

        public void Dispose()
        {
            _zkClient.Dispose();
        }
    }
}
