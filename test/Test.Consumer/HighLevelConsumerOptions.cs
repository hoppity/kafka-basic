using CommandLine;

namespace Consumer
{
    [Verb("balanced")]
    internal class HighLevelConsumerOptions
    {
        [Option('z', "zkconnect", Required = true, HelpText = "The Zookeeper connection string - e.g. 192.168.33.10:2181.")]
        public string ZkConnect { get; set; }
        [Option('g', "group", Required = true, HelpText = "The name of the consumer group.")]
        public string Group { get; set; }
        [Option('t', "topic", Required = true, HelpText = "The name of the topic.")]
        public string Topic { get; set; }
        [Option('b', "batch", Required = false, HelpText = "Whether to use a batched consumer.", Default = false)]
        public bool Batch { get; set; }
        [Option('s', "batchSize", Required = false, HelpText = "The maximum batch size to received.", Default = 1000)]
        public int BatchSize { get; set; }
        [Option('i', "batchTimeoutMs", Required = false, HelpText = "The time to wait for a batch to accumulate.", Default = 100)]
        public int BatchTimeoutMs { get; set; }
    }
}