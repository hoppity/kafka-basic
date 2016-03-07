namespace SimpleKafka
{
    public interface IKafkaConsumer
    {
        KafkaConsumerInstance Join(ConsumerOptions options = null);
    }

    public class KafkaConsumer : IKafkaConsumer
    {
        private readonly string _zkConnect;
        private readonly string _groupName;

        public KafkaConsumer(string zkConnect, string groupName)
        {
            _zkConnect = zkConnect;
            _groupName = groupName;
        }

        public KafkaConsumerInstance Join(ConsumerOptions options = null)
        {
            return new KafkaConsumerInstance(_zkConnect, _groupName, options ?? new ConsumerOptions());
        }
    }
}
