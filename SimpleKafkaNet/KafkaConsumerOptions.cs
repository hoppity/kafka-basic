using Kafka.Client.Requests;

namespace SimpleKafkaNet
{
    public class KafkaConsumerOptions
    {
        public bool AutoCommitEnabled { get; set; }
        public string AutoOffsetReset { get; set; }
    }
}
