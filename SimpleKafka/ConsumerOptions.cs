using Kafka.Client.Cfg;
using Kafka.Client.Requests;

namespace SimpleKafka
{
    public class ConsumerOptions
    {
        public bool AutoCommit { get; set; }
        public AutoOffsetReset AutoOffsetReset { get; set; }
        public int BackOffTimeMs { get; set; }

        internal ConsumerConfiguration ToConfiguration(string zkConnect, string groupName)
        {
            string reset;
            switch (AutoOffsetReset)
            {
                case AutoOffsetReset.Largest:
                    reset = OffsetRequest.LargestTime;
                    break;
                default:
                    reset = OffsetRequest.SmallestTime;
                    break;
            }

            return new ConsumerConfiguration
            {
                AutoCommit = AutoCommit,
                GroupId = groupName,
                ZooKeeper = new ZooKeeperConfiguration(
                    zkConnect,
                    ZooKeeperConfiguration.DefaultSessionTimeout,
                    ZooKeeperConfiguration.DefaultConnectionTimeout,
                    ZooKeeperConfiguration.DefaultSyncTime
                    ),
                AutoOffsetReset = reset,
                BackOffIncrement = BackOffTimeMs
            };
        }
    }
}