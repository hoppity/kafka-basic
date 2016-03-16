using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Cfg;
using Kafka.Client.Cluster;
using Kafka.Client.Producers;
using Kafka.Client.Utils;
using Kafka.Client.ZooKeeperIntegration;

namespace Kafka.Basic
{
    public interface IZookeeperClient : IDisposable
    {
        IEnumerable<Broker> GetAllBrokers();
        IProducer<TKey, TMessage> CreateProducer<TKey, TMessage>();
    }

    public class ZookeeperClient : IZookeeperClient
    {
        private readonly ZooKeeperClient _client;

        public ZookeeperClient(string zkConnect)
        {
            _client = new ZooKeeperClient(
                zkConnect,
                ZooKeeperConfiguration.DefaultSessionTimeout,
                ZooKeeperStringSerializer.Serializer
                );
            _client.Connect();
        }

        public IEnumerable<Broker> GetAllBrokers()
        {
            return ZkUtils.GetAllBrokersInCluster(_client);
        }

        public IProducer<TKey, TMessage> CreateProducer<TKey, TMessage>()
        {
            var config = new ProducerConfiguration(
                GetAllBrokers()
                    .Select(b => new BrokerConfiguration
                    {
                        BrokerId = b.Id,
                        Host = b.Host,
                        Port = b.Port
                    }).ToList()
                );

            return new Producer<TKey, TMessage>(config);
        }

        public void Dispose()
        {
            _client.Dispose();
        }
    }
}
