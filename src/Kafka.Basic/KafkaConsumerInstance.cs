﻿using System;
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
        IKafkaConsumerStream Subscribe(string topicName);
        IKafkaBatchedConsumerStream Subscribe(string topicName, int batchSize, int timeoutMs);
        Task Commit();
        void Commit(string topic, int partition, long offset);
        Task Shutdown();
    }

    public class KafkaConsumerInstance : IKafkaConsumerInstance
    {
        private readonly IList<IKafkaConsumerStream> _streams = new List<IKafkaConsumerStream>();
        private readonly IZookeeperConsumerConnector _balancedConsumer;

        public KafkaConsumerInstance(ConsumerConfiguration config)
        {
            _balancedConsumer = CreateZookeeperConnector(config);
        }

        public KafkaConsumerInstance(IZookeeperConsumerConnector connector)
        {
            _balancedConsumer = connector;
        }

        public KafkaConsumerInstance(IZookeeperConnection zkConnect, ConsumerOptions options)
        {
            _balancedConsumer = zkConnect.CreateConsumerConnector(options);
        }

        private ZookeeperConsumerConnector CreateZookeeperConnector(ConsumerConfiguration config)
        {
            return new ZookeeperConsumerConnector(config, true);
        }

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

        public IKafkaBatchedConsumerStream Subscribe(string topicName, int batchSize, int timeoutMs)
        {
            var stream = Subscribe(topicName);
            var consumerStream = new KafkaBatchedConsumerStream(this, stream, topicName, batchSize, timeoutMs);
            return consumerStream;
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

        public void Commit(string topic, int partition, long offset)
        {
            _balancedConsumer.CommitOffset(topic, partition, offset, false);
        }

    }
}