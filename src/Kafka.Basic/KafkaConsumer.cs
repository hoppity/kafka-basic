namespace Kafka.Basic
{
    public interface IKafkaConsumer
    {
        KafkaConsumerInstance Join();
    }

    public class KafkaConsumer : IKafkaConsumer
    {
        private readonly IZookeeperConnection _zkConnect;
        private readonly string _groupName;

        public KafkaConsumer(IZookeeperConnection zkConnect, string groupName)
        {
            _zkConnect = zkConnect;
            _groupName = groupName;
        }

        public KafkaConsumerInstance Join()
        {
            return new KafkaConsumerInstance(_zkConnect, _groupName);
        }
    }
}
