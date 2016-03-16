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

        public void Dispose()
        {
            _client.Dispose();
        }
    }
}
