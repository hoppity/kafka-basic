using CommandLine;

namespace Producer
{
    internal class Options
    {
        [Option('z', "zkconnect", Required = true, HelpText = "The Zookeeper connection string - e.g. 192.168.33.10:2181.")]
        public string ZkConnect { get; set; }
        [Option('t', "topic", Required = true, HelpText = "The name of the topic.")]
        public string Topic { get; set; }
        [Option('b', "batchsize", Required = false, HelpText = "The size of each batch published.", Default = 100)]
        public int BatchSize { get; set; }
        [Option('m', "messages", Required = false, HelpText = "The number of messages to publish.", Default = long.MaxValue)]
        public long  Messages { get; set; }
        [Option('s', "sleep", Required = false, HelpText = "Time to wait between publishes.", Default = 10)]
        public int Sleep { get; set; }
    }
}
