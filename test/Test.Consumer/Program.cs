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
                    ChaosMonkeyOptions
                    >(args)
                .MapResult(
                    (HighLevelConsumerOptions opts) => new HighLevelConsumer().Start(opts),
                    (SimpleConsumerOptions opts) => new SimpleConsumer().Start(opts),
                    (BatchedConsumerOptions opts) => new BatchedConsumer().Start(opts),
                    (ChaosMonkeyOptions opts) => new ChaosMonkey().Start(opts),
                    errs => 1
                );
        }
    }
}
