using CommandLine;

namespace Consumer
{
    [Verb("chaos")]
    internal class ChaosMonkeyOptions
    {
        [Option('z', "zkconnect", Required = true, HelpText = "The Zookeeper connection string - e.g. 192.168.33.10:2181.")]
        public string ZkConnect { get; set; }
        [Option('g', "group", Required = true, HelpText = "The name of the consumer group.")]
        public string Group { get; set; }
        [Option('t', "topic", Required = true, HelpText = "The name of the topic.")]
        public string Topic { get; set; }
        [Option('s', "batchSizeMax", Required = false, HelpText = "The maximum size of the batch.", Default = Kafka.Basic.BatchedConsumer.DefaultBatchSizeMax)]
        public int BatchSizeMax { get; set; }
        [Option('b', "batchTimeoutMs", Required = false, HelpText = "The maximum time to wait for messages to batch.", Default = Kafka.Basic.BatchedConsumer.DefaultBatchTimeoutMs)]
        public int BatchTimeoutMs { get; set; }
    }
}