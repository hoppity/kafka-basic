using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Serialization;

namespace Kafka.Basic
{
    public interface IKafkaConsumerInstance : IDisposable
    {
        KafkaConsumerStream Subscribe(string topicName);
        Task Commit();
        Task Shutdown();
    }

    public class KafkaConsumerInstance : IKafkaConsumerInstance
    {
        private readonly IList<IKafkaConsumerStream> _streams = new List<IKafkaConsumerStream>();
        private readonly ZookeeperConsumerConnector _balancedConsumer;

        public KafkaConsumerInstance(ConsumerConfiguration config)
        {
            _balancedConsumer = CreateZookeeperConnector(config);
        }


        public KafkaConsumerInstance(ZookeeperConsumerConnector connector)
        {
            _balancedConsumer = connector;
        }

        public KafkaConsumerInstance(string zkConnect, string groupName)
        {
            _balancedConsumer = CreateZookeeperConnector(zkConnect, groupName);
        }


        private ConsumerConfiguration CreateConsumerConfiguration(string zkConnect, string groupName)
        {
            return new ConsumerConfiguration
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
        }


        private ZookeeperConsumerConnector CreateZookeeperConnector(ConsumerConfiguration config)
        {
            return new ZookeeperConsumerConnector(config, true,
                OnRebalance,
                OnZkDisconnect,
                OnZkExpired
            );
        }


        private ZookeeperConsumerConnector CreateZookeeperConnector(string zkConnect, string groupName)
        {
            return new ZookeeperConsumerConnector(
                CreateConsumerConfiguration(zkConnect, groupName),
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

        public Task Shutdown()
        {
            return Task.Run(() =>
            {
                foreach (var stream in _streams)
                {
                    CloseStream(stream);
                }
                _streams.Clear();
                _balancedConsumer.CommitOffsets();
                _balancedConsumer.ReleaseAllPartitionOwnerships();
            });
        }

        private void CloseStream(IKafkaConsumerStream stream)
        {
            stream.Shutdown();
            stream.Dispose();
        }

        public void Dispose()
        {
            if (_streams.Any())
            {
                Shutdown();
            }
            _balancedConsumer.Dispose();
        }

        public Task Commit()
        {
            return Task.Run(() => _balancedConsumer.CommitOffsets());
        }

    }
}