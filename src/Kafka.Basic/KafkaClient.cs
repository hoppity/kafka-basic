using System;

namespace Kafka.Basic
{
    public interface IKafkaClient : IDisposable
    {
        IKafkaTopic Topic(string name);
        IKafkaConsumer Consumer(string groupName);
        IKafkaConsumer Consumer(ConsumerOptions options);
    }

    public class KafkaClient : IKafkaClient
    {
        private readonly IZookeeperConnection _zkConnection;

        public KafkaClient(string zkConnect)
        {
            _zkConnection = new ZookeeperConnection(zkConnect);
        }

        public IKafkaTopic Topic(string name)
        {
            return new KafkaTopic(_zkConnection, name);
        }

        public IKafkaConsumer Consumer(string groupName)
        {
            return new KafkaConsumer(_zkConnection, new ConsumerOptions { GroupName = groupName });
        }

        public IKafkaConsumer Consumer(ConsumerOptions options)
        {
            return new KafkaConsumer(_zkConnection, options);
        }

        public IKafkaSimpleConsumer SimpleConsumer()
        {
            return new KafkaSimpleConsumer(_zkConnection);
        }

        public void Dispose() { }
    }
}
