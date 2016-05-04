namespace Kafka.Basic
{
    public class ProducerConfig
    {
        public const short DefaultAcks = 1;

        public short Acks { get; set; }

        public static ProducerConfig Default()
        {
            return new ProducerConfig
            {
                Acks = DefaultAcks
            };
        }

    }
}