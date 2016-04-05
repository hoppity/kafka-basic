namespace Kafka.Basic
{
    public interface IKafkaConsumer
    {
        IKafkaConsumerInstance Join();
    }

    public class KafkaConsumer : IKafkaConsumer
    {
        private readonly IZookeeperConnection _zkConnect;
        private readonly ConsumerOptions _options;

        public KafkaConsumer(IZookeeperConnection zkConnect, ConsumerOptions options)
        {
            _zkConnect = zkConnect;
            _options = options;
        }

        public IKafkaConsumerInstance Join()
        {
            return new KafkaConsumerInstance(_zkConnect, _options);
        }
    }
}
