namespace Kafka.Basic
{
    public class ConsumedMessage : Message
    {
        public int Partition { get; set; }
        public long Offset { get; set; }
    }
}
