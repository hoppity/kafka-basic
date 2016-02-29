using System.ComponentModel;
using Kafka.Client.Cfg;

namespace SimpleKafkaNet
{
    public class KafkaClient
    {
        private const int ZkSessionTimeoutMs = 30000;
        private const int ZkConnectionTimeoutMs = 30000;
        private const int ZkSyncTimeMs = 2000;

        private ZooKeeperConfiguration _zkConfig;

        public KafkaClient(string zkConnect)
        {
            _zkConfig = new ZooKeeperConfiguration(zkConnect, ZkSessionTimeoutMs, ZkConnectionTimeoutMs, ZkSyncTimeMs);
        }

        public IKafkaConsumer Consumer(string groupName)
        {
            return new KafkaConsumer(_zkConfig, groupName);
        }
    }
}
