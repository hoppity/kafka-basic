using System;
using Kafka.Client.Cfg;

namespace Kafka.Basic
{
    public interface IKafkaClient : IDisposable
    {
        IKafkaTopic Topic(string name);
        IKafkaConsumer Consumer(string groupName);
    }

    public class KafkaClient : IKafkaClient
    {
        private readonly string _zkConnect;
        private readonly IZookeeperClient _zkClient;

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

            _zkClient = new ZookeeperClient(_zkConnect);
        }

        public IKafkaTopic Topic(string name)
        {
            return new KafkaTopic(_zkClient, name);
        }

        public IKafkaConsumer Consumer(string groupName)
        {
            return new KafkaConsumer(_zkConnect, groupName);
        }

        public IKafkaSimpleConsumer SimpleConsumer()
        {
            return new KafkaSimpleConsumer(_zkConnect);
        }

        public void Dispose()
        {
            _zkClient.Dispose();
        }
    }
}
