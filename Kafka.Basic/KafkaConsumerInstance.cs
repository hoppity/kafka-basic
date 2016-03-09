using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Serialization;

namespace Kafka.Basic
{
    public interface IKafkaConsumerInstance : IDisposable
    {
        KafkaConsumerStream Subscribe(string topicName);
    }

    public class KafkaConsumerInstance : IKafkaConsumerInstance
    {
        private readonly IList<IKafkaConsumerStream> _streams = new List<IKafkaConsumerStream>();
        private readonly ZookeeperConsumerConnector _balancedConsumer;

        public KafkaConsumerInstance(string zkConnect, string groupName)
        {
            var config = new ConsumerConfiguration
            {
                AutoCommit = false,
                GroupId = groupName,
                ZooKeeper = new ZooKeeperConfiguration(
                    zkConnect,
                    ZooKeeperConfiguration.DefaultSessionTimeout,
                    ZooKeeperConfiguration.DefaultConnectionTimeout,
                    ZooKeeperConfiguration.DefaultSyncTime
                    )
            };

            _balancedConsumer = new ZookeeperConsumerConnector(
                config,
                true,
                OnRebalance,
                OnZkDisconnect,
                OnZkExpired
                );
        }

        public KafkaConsumerStream Subscribe(string topicName)
        {
            var streams = _balancedConsumer.CreateMessageStreams(
                new Dictionary<string, int>
                {
                    {topicName, 1}
                },
                new DefaultDecoder()
                );

            var stream = streams[topicName][0];

            var consumerStream = new KafkaConsumerStream(stream);
            _streams.Add(consumerStream);
            return consumerStream;
        }

        private void OnZkExpired(object sender, EventArgs e)
        {
            Console.WriteLine($"{DateTime.Now.ToString("s")}: ZK_EXPIRED");
        }

        private void OnZkDisconnect(object sender, EventArgs e)
        {
            Console.WriteLine($"{DateTime.Now.ToString("s")}: ZK_DISCONNECT");
        }

        private void OnRebalance(object sender, EventArgs e)
        {
            Console.WriteLine($"{DateTime.Now.ToString("s")}: ZK_REBALANCE");
        }

        public void Shutdown()
        {
            foreach (var stream in _streams)
            {
                stream.Shutdown();
                stream.Dispose();
            }
            _streams.Clear();
            _balancedConsumer.CommitOffsets();
            _balancedConsumer.ReleaseAllPartitionOwnerships();
        }

        public void Dispose()
        {
            if (_streams.Any())
            {
                Shutdown();
            }
            _balancedConsumer.Dispose();
        }
    }
}