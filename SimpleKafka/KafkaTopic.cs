using System;
using System.Linq;
using System.Text;
using Kafka.Client.Cfg;
using Kafka.Client.Producers;
using Kafka.Client.Utils;
using Kafka.Client.ZooKeeperIntegration;
using KafkaMessage = Kafka.Client.Messages.Message;

namespace SimpleKafka
{
    public interface IKafkaTopic : IDisposable
    {
        void Send(params Message[] messages);
    }

    public class KafkaTopic : IKafkaTopic
    {
        private readonly string _name;
        private readonly Producer<string, KafkaMessage> _producer;

        public KafkaTopic(IZooKeeperClient zkClient, string name)
        {
            _name = name;
            var brokers = ZkUtils.GetAllBrokersInCluster(zkClient);
            _producer = new Producer<string, KafkaMessage>(
                new ProducerConfiguration(
                    brokers
                        .Select(b => new BrokerConfiguration
                        {
                            BrokerId = b.Id,
                            Host = b.Host,
                            Port = b.Port
                        }).ToList()
                    )
                );
        }

        public void Send(params Message[] messages)
        {
            _producer.Send(
                messages
                    .Select(m => new ProducerData<string, KafkaMessage>(
                        _name,
                        m.Key,
                        new KafkaMessage(Encoding.UTF8.GetBytes(m.Value)))
                    )
                );
        }

        public void Dispose()
        {
            _producer.Dispose();
        }
    }
}