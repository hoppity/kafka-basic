namespace Kafka.Basic
{
    public class ConsumerOptions
    {
        public ConsumerOptions()
        {
            AutoOffsetReset = Offset.Earliest;
            AutoCommit = true;
            Batch = false;
            MaxBatchSize = 1000;
            MaxBatchTimeoutMs = 100;
        }

        public string GroupName { get; set; }
        public bool AutoCommit { get; set; }
        public Offset AutoOffsetReset { get; set; }
        public bool Batch { get; set; }
        public int MaxBatchSize { get; set; }
        public int MaxBatchTimeoutMs { get; set; }
    }
}