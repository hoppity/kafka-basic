using CommandLine;

namespace Consumer
{
    [Verb("auto-balanced")]
    internal class AutoBalancedConsumerOptions
    {
        [Option('z', "zkconnect", Required = true, HelpText = "The Zookeeper connection string - e.g. 192.168.33.10:2181.")]
        public string ZkConnect { get; set; }
        [Option('g', "group", Required = true, HelpText = "The name of the consumer group.")]
        public string Group { get; set; }
        [Option('t', "topic", Required = true, HelpText = "The name of the topic.")]
        public string Topic { get; set; }
    }
}