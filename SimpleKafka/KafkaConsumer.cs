namespace SimpleKafka
{
    public interface IKafkaConsumer
    {
        KafkaConsumerInstance Join();
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

        public KafkaConsumerInstance Join()
        {
            return new KafkaConsumerInstance(_zkConnect, _groupName);
        }
    }
}
