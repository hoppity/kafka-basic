using CommandLine;

namespace Consumer
{
    [Verb("simple")]
    internal class SimpleConsumerOptions
    {
        [Option('z', "zkconnect", Required = true, HelpText = "The Zookeeper connection string - e.g. 192.168.33.10:2181.")]
        public string ZkConnect { get; set; }
        [Option('t', "topic", Required = true, HelpText = "The name of the topic.")]
        public string Topic { get; set; }
        [Option('p', "partition", Required = true, HelpText = "The identifier of the partition.")]
        public int Partition { get; set; }
        [Option('o', "offset", HelpText = "The starting offset (-2 = earliest, -1 = latest).", Default = Kafka.Basic.Offset.Latest)]
        public long Offset { get; set; }
    }
}