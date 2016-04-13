using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Serialization;
using Kafka.Client.ZooKeeperIntegration.Events;

namespace Kafka.Basic
{
    public interface IKafkaConsumerInstance : IDisposable
    {
        event EventHandler<ConsumerRebalanceEventArgs> Rebalanced;
        event EventHandler ZookeeperDisconnected;
        event EventHandler ZookeeperSessionExpired;

        string Id { get; }
        IKafkaConsumerStream Subscribe(string topicName);
        IEnumerable<IKafkaConsumerStream> Subscribe(string topicName, int threads);
        void Commit();
        void Commit(string topic, int partition, long offset);
        void Shutdown();
    }

    public class KafkaConsumerInstance : IKafkaConsumerInstance
    {
        private readonly List<IKafkaConsumerStream> _streams = new List<IKafkaConsumerStream>();
        private readonly IConsumerConnector _consumerConnector;

        public string Id => _consumerConnector.ConsumerId;

        public KafkaConsumerInstance(IZookeeperConnection zkConnect, ConsumerOptions options)
        {
            _consumerConnector = zkConnect.CreateConsumerConnector(options);
            _consumerConnector.Rebalanced += OnRebalanced;
            _consumerConnector.ZookeeperDisconnected += OnZookeeperDisconnected;
            _consumerConnector.ZookeeperSessionExpired += OnZookeeperSessionExpired;
        }

        protected virtual void OnZookeeperSessionExpired(object sender, EventArgs e)
        {
            ZookeeperSessionExpired?.Invoke(sender, e);
        }

        protected virtual void OnZookeeperDisconnected(object sender, EventArgs e)
        {
            ZookeeperDisconnected?.Invoke(sender, e);
        }

        protected virtual void OnRebalanced(object sender, ConsumerRebalanceEventArgs e)
        {
            Rebalanced?.Invoke(sender, e);
        }

        public event EventHandler<ConsumerRebalanceEventArgs> Rebalanced;
        public event EventHandler ZookeeperDisconnected;
        public event EventHandler ZookeeperSessionExpired;

        public IKafkaConsumerStream Subscribe(string topicName)
        {
            return Subscribe(topicName, 1).First();
        }

        public IEnumerable<IKafkaConsumerStream> Subscribe(string topicName, int threads)
        {
            var streams = _consumerConnector.CreateMessageStreams(
                new Dictionary<string, int>
                {
                    {topicName, threads}
                },
                new DefaultDecoder()
            );

            var consumerStreams = streams[topicName]
                .Select(s => new KafkaConsumerStream(s))
                .ToArray();
            _streams.AddRange(consumerStreams);

            return consumerStreams;
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
            _consumerConnector.ReleaseAllPartitionOwnerships();
        }

        private void CloseStream(IKafkaConsumerStream stream)
        {
            stream.Shutdown();
            stream.Dispose();
        }

        public void Commit()
        {
            _consumerConnector.CommitOffsets();
        }

        public void Commit(string topic, int partition, long offset)
        {
            _consumerConnector.CommitOffset(topic, partition, offset, false);
        }

        public void Dispose()
        {
            if (_streams.Any())
            {
                Shutdown();
            }
            _consumerConnector.Dispose();
        }
    }
}