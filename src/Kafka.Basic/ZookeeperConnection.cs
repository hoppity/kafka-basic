using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Helper;

namespace Kafka.Basic
{
    public interface IZookeeperConnection
    {
        IZookeeperClient CreateClient();
        IZookeeperConsumerConnector CreateConsumerConnector(ConsumerOptions options);
        KafkaSimpleManager<string, Message> CreateSimpleManager();
    }

    public class ZookeeperConnection : IZookeeperConnection
    {
        private readonly string _zkConnect;

        public ZookeeperConnection(string zkConnect)
        {
            _zkConnect = zkConnect;

            var kafkaConfig = new KafkaSimpleManagerConfiguration()
            {
                Zookeeper = zkConnect,
                MaxMessageSize = SyncProducerConfiguration.DefaultMaxMessageSize,
                PartitionerClass = ProducerConfiguration.DefaultPartitioner
            };
            kafkaConfig.Verify();
        }

        public IZookeeperClient CreateClient()
        {
            return new ZookeeperClient(_zkConnect);
        }

        public IZookeeperConsumerConnector CreateConsumerConnector(ConsumerOptions options)
        {
            var config = new ConsumerConfiguration
            {
                AutoCommit = !options.Batch && options.AutoCommit,
                GroupId = options.GroupName,
                ZooKeeper = new ZooKeeperConfiguration(
                    _zkConnect,
                    ZooKeeperConfiguration.DefaultSessionTimeout,
                    ZooKeeperConfiguration.DefaultConnectionTimeout,
                    ZooKeeperConfiguration.DefaultSyncTime
                    ),
                BackOffIncrement = 0,
                MaxFetchWaitMs = 1000
            };
            return new ZookeeperConsumerConnector(config, true);
        }

        public KafkaSimpleManager<string, Message> CreateSimpleManager()
        {
            return new KafkaSimpleManager<string, Message>(
                new KafkaSimpleManagerConfiguration
                {
                    Zookeeper = _zkConnect
                });
        }
    }
}
