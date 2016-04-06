using CommandLine;

namespace Consumer
{
    class Program
    {
        static int Main(string[] args)
        {
            return Parser.Default
                .ParseArguments<
                    HighLevelConsumerOptions,
                    SimpleConsumerOptions,
                    BatchedConsumerOptions,
                    AutoBalancedConsumerOptions
                    >(args)
                .MapResult(
                    (HighLevelConsumerOptions opts) => new HighLevelConsumer().Start(opts),
                    (SimpleConsumerOptions opts) => new SimpleConsumer().Start(opts),
                    (BatchedConsumerOptions opts) => new BatchedConsumer().Start(opts),
                    (AutoBalancedConsumerOptions opts) => new AutoBalancedConsumer().Start(opts),
                    errs => 1
                );
        }
    }
}
