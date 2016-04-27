using CommandLine;

namespace Consumer
{
    [Verb("batched")]
    internal class BatchedConsumerOptions
    {
        [Option('z', "zkconnect", Required = true, HelpText = "The Zookeeper connection string - e.g. 192.168.33.10:2181.")]
        public string ZkConnect { get; set; }
        [Option('g', "group", Required = true, HelpText = "The name of the consumer group.")]
        public string Group { get; set; }
        [Option('t', "topic", Required = true, HelpText = "The name of the topic.")]
        public string Topic { get; set; }
        [Option('h', "threads", Required = false, HelpText = "The number of consumer threads.", Default = Kafka.Basic.BatchedConsumer.DefaultNumberOfThreads)]
        public int Threads { get; set; }
        [Option('b', "batchTimeoutMs", Required = false, HelpText = "The maximum time to wait for messages to batch.", Default = Kafka.Basic.BatchedConsumer.DefaultBatchTimeoutMs)]
        public int BatchTimeoutMs { get; set; }
        [Option('s', "maxBatchSize", Required = false, HelpText = "The maximum number of messages in a batch.", Default = Kafka.Basic.BatchedConsumer.DefaultBatchTimeoutMs)]
        public int MaxBatchSize { get; set; }
    }
}