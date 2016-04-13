using System;
using System.Collections.Generic;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Serialization;
using Kafka.Client.ZooKeeperIntegration.Events;

namespace Kafka.Basic
{
    public interface IConsumerConnector : IDisposable
    {
        event EventHandler Rebalancing;
        event EventHandler<ConsumerRebalanceEventArgs> Rebalanced;
        event EventHandler ZookeeperDisconnected;
        event EventHandler ZookeeperSessionExpired;

        string ConsumerId { get; }

        IDictionary<string, IList<IKafkaMessageStream<Client.Messages.Message>>> CreateMessageStreams(
            IDictionary<string, int> topicThreadCount,
            IDecoder<Client.Messages.Message> decoder
            );

        void CommitOffsets();
        void ReleaseAllPartitionOwnerships();
        void CommitOffset(string topic, int partition, long offset, bool setPosition);
    }

    internal class ConsumerConnector : IConsumerConnector
    {
        private readonly IZookeeperConsumerConnector _connector;
        public string ConsumerId => _connector.GetConsumerIdString();

        public event EventHandler Rebalancing;
        public event EventHandler<ConsumerRebalanceEventArgs> Rebalanced;
        public event EventHandler ZookeeperDisconnected;
        public event EventHandler ZookeeperSessionExpired;

        public ConsumerConnector(ConsumerConfiguration config)
        {
            _connector = new ZookeeperConsumerConnector(config, true, OnRebalancing, OnZookeeperDisconnected, OnZookeeperSessionExpired, OnRebalanced);
        }

        public IDictionary<string, IList<IKafkaMessageStream<Client.Messages.Message>>> CreateMessageStreams(IDictionary<string, int> topicThreadCount, IDecoder<Client.Messages.Message> decoder)
        {
            return _connector.CreateMessageStreams(topicThreadCount, decoder);
        }

        public void CommitOffsets()
        {
            _connector.CommitOffsets();
        }

        public void ReleaseAllPartitionOwnerships()
        {
            _connector.ReleaseAllPartitionOwnerships();
        }

        public void CommitOffset(string topic, int partition, long offset, bool setPosition)
        {
            _connector.CommitOffset(topic, partition, offset, setPosition);
        }

        protected virtual void OnRebalancing(object sender, EventArgs e)
        {
            Rebalancing?.Invoke(sender, e);
        }

        protected virtual void OnRebalanced(object sender, ConsumerRebalanceEventArgs e)
        {
            Rebalanced?.Invoke(sender, e);
        }

        protected virtual void OnZookeeperDisconnected(object sender, EventArgs e)
        {
            ZookeeperDisconnected?.Invoke(sender, e);
        }

        protected virtual void OnZookeeperSessionExpired(object sender, EventArgs e)
        {
            ZookeeperSessionExpired?.Invoke(sender, e);
        }

        public void Dispose()
        {
            _connector.Dispose();
        }
    }
}
