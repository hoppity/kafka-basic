using System;
using System.Threading;
using Kafka.Basic;
using Kafka.Basic.Auto;
using IBalancedConsumer = Kafka.Basic.Auto.IBalancedConsumer;

namespace Consumer
{
    class AutoBalancedConsumer : ConsoleApp
    {
        private static AutoBalancedConsumerOptions Options;

        public int Start(AutoBalancedConsumerOptions opts)
        {
            Options = opts;

            var host = new KafkaHost(opts.ZkConnect);
            
            var handle = new AutoResetEvent(false);
            ListenToConsole(() =>
            {
                host.Shutdown();
                handle.Set();
            });

            host.Start(typeof(Consumer));

            handle.WaitOne();

            return 0;
        }

        class Consumer : IBalancedConsumer
        {
            public string Group { get { return Options.Group; } }
            public string Topic { get { return Options.Topic; } }

            public void Receive(ConsumedMessage message)
            {
                Console.WriteLine($"P:{message.Partition},O:{message.Offset}");
            }
        }
    }
}
