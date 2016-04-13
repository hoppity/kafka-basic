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
        [Option('h', "threads", Required = false, HelpText = "The number of consumer threads to run.", Default = 1)]
        public int Threads { get; set; }
    }
}