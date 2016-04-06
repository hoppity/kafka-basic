using System;
using System.Collections.Generic;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Serialization;

namespace Kafka.Basic
{
    public interface IBalancedConsumer : IDisposable
    {
        event EventHandler Rebalanced;
        event EventHandler ZookeeperDisconnected;
        event EventHandler ZookeeperSessionExpired;

        IDictionary<string, IList<IKafkaMessageStream<Client.Messages.Message>>> CreateMessageStreams(
            IDictionary<string, int> topicThreadCount,
            IDecoder<Client.Messages.Message> decoder
            );

        void CommitOffsets();
        void ReleaseAllPartitionOwnerships();
        void CommitOffset(string topic, int partition, long offset, bool setPosition);
    }

    internal class BalancedConsumer : IBalancedConsumer
    {
        private readonly IZookeeperConsumerConnector _connector;

        public event EventHandler Rebalanced;
        public event EventHandler ZookeeperDisconnected;
        public event EventHandler ZookeeperSessionExpired;

        public BalancedConsumer(ConsumerConfiguration config)
        {
            _connector = new ZookeeperConsumerConnector(config, true, OnRebalanced, OnZookeeperDisconnected, OnZookeeperSessionExpired);
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

        protected virtual void OnRebalanced(object sender, EventArgs e)
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
