using System;
using System.Linq;
using Kafka.Client.Cfg;
using Kafka.Client.Producers;
using KafkaMessage = Kafka.Client.Messages.Message;

namespace Kafka.Basic
{
    public interface IKafkaTopic : IDisposable
    {
        void Send(params Message[] messages);
    }

    public class KafkaTopic : IKafkaTopic
    {
        private readonly string _name;
        private readonly IProducer<string, KafkaMessage> _producer;
        private IZookeeperClient _zkClient;

        public KafkaTopic(IZookeeperConnection zkConnect, string name)
        {
            _name = name;
            _zkClient = zkConnect.CreateClient();
            _producer = _zkClient.CreateProducer<string, KafkaMessage>();
        }

        public void Send(params Message[] messages)
        {
            _producer.Send(
                messages.Select(m => m.AsProducerData(_name))
                );
        }

        public void Dispose()
        {
            _producer.Dispose();
            _zkClient.Dispose();
        }
    }
}