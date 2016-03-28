namespace Kafka.Basic
{
    public class ConsumerOptions
    {
        public ConsumerOptions()
        {
            AutoOffsetReset = Offset.Earliest;
            AutoCommit = true;
        }

        public string GroupName { get; set; }
        public bool AutoCommit { get; set; }
        public Offset AutoOffsetReset { get; set; }
    }
}