using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Serialization;

namespace Kafka.Basic
{
    public interface IKafkaConsumerInstance : IDisposable
    {
        event EventHandler Rebalanced;
        event EventHandler ZookeeperDisconnected;
        event EventHandler ZookeeperSessionExpired;

        IKafkaConsumerStream Subscribe(string topicName);
        void Commit();
        void Commit(string topic, int partition, long offset);
        void Shutdown();
    }

    public class KafkaConsumerInstance : IKafkaConsumerInstance
    {
        private readonly IList<IKafkaConsumerStream> _streams = new List<IKafkaConsumerStream>();
        private readonly IBalancedConsumer _balancedConsumer;

        public KafkaConsumerInstance(IZookeeperConnection zkConnect, ConsumerOptions options)
        {
            _balancedConsumer = zkConnect.CreateConsumerConnector(options);
            _balancedConsumer.Rebalanced += OnRebalanced;
            _balancedConsumer.ZookeeperDisconnected += OnZookeeperDisconnected;
            _balancedConsumer.ZookeeperSessionExpired += OnZookeeperSessionExpired;
        }

        protected virtual void OnZookeeperSessionExpired(object sender, EventArgs e)
        {
            ZookeeperSessionExpired?.Invoke(sender, e);
        }

        protected virtual void OnZookeeperDisconnected(object sender, EventArgs e)
        {
            ZookeeperDisconnected?.Invoke(sender, e);
        }

        protected virtual void OnRebalanced(object sender, EventArgs e)
        {
            Rebalanced?.Invoke(sender, e);
        }

        public event EventHandler Rebalanced;
        public event EventHandler ZookeeperDisconnected;
        public event EventHandler ZookeeperSessionExpired;

        public IKafkaConsumerStream Subscribe(string topicName)
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

        public void Shutdown()
        {
            lock (this)
            {
                foreach (var stream in _streams)
                {
                    CloseStream(stream);
                }
            }
            _streams.Clear();
            _balancedConsumer.ReleaseAllPartitionOwnerships();
        }

        private void CloseStream(IKafkaConsumerStream stream)
        {
            stream.Shutdown();
            stream.Dispose();
        }

        public void Commit()
        {
            _balancedConsumer.CommitOffsets();
        }

        public void Commit(string topic, int partition, long offset)
        {
            _balancedConsumer.CommitOffset(topic, partition, offset, false);
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