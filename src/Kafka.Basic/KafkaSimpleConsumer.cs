using System;
using System.Collections.Generic;

namespace Kafka.Basic
{
    public interface IKafkaSimpleConsumer : IDisposable
    {
        IKafkaConsumerStream Subscribe(string topicName, int partition, long offset = -2);
    }

    public class KafkaSimpleConsumer : IKafkaSimpleConsumer
    {
        private readonly string _zkConnect;
        private readonly IList<IKafkaConsumerStream> _streams = new List<IKafkaConsumerStream>();

        public KafkaSimpleConsumer(string zkConnect)
        {
            _zkConnect = zkConnect;
        }

        public void Dispose()
        {
            foreach (var stream in _streams)
            {
                stream.Dispose();
            }
            _streams.Clear();
        }

        public IKafkaConsumerStream Subscribe(string topicName, int partition, long offset = -2)
        {
            var stream = new KafkaSimpleConsumerStream(_zkConnect, topicName, partition, offset);
            _streams.Add(stream);
            return stream;
        }
    }

    public enum Offset : long
    {
        Earliest = -2,
        Latest = -1
    }
}