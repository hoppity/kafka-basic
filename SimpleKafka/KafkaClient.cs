using System;
using Kafka.Client.Cfg;
using Kafka.Client.ZooKeeperIntegration;

namespace SimpleKafka
{
    public interface IKafkaClient : IDisposable
    {
        IKafkaTopic Topic(string name);
        IKafkaConsumer Consumer(string groupName);
        void Dispose();
    }

    public class KafkaClient : IKafkaClient
    {
        private readonly string _zkConnect;
        private readonly ZooKeeperClient _zkClient;

        public KafkaClient(string zkConnect)
        {
            _zkConnect = zkConnect;
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

        public IKafkaConsumer Consumer(string groupName)
        {
            return new KafkaConsumer(_zkConnect, groupName);
        }

        public void Dispose()
        {
            _zkClient.Dispose();
        }
    }
}
