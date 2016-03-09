using CommandLine;

namespace Consumer
{
    class Program
    {
        static int Main(string[] args)
        {
            return Parser.Default
                .ParseArguments<HighLevelConsumerOptions, SimpleConsumerOptions>(args)
                .MapResult(
                    (HighLevelConsumerOptions opts) => new HighLevelConsumer().Start(opts),
                    (SimpleConsumerOptions opts) => new SimpleConsumer().Start(opts),
                    errs => 1
                );
        }
    }
}
