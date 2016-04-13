using System;
using System.Threading;
using Kafka.Basic;
using Kafka.Basic.Abstracted;
using Metrics;

namespace Consumer
{
    class HighLevelConsumer
    {
        private Thread _consoleThread;

        public int Start(HighLevelConsumerOptions opts)
        {
            var timer = Metric.Timer("Received", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            using (var client = new KafkaClient(opts.ZkConnect))
            using (var consumer = new BalancedConsumer(client, opts.Group, opts.Topic, opts.Threads))
            {
                ListenToConsole(consumer);
                consumer.Start(message =>
                {
                    var time = DateTime.UtcNow.Ticks;
                    var value = long.Parse(message.Value);
                    var diff = (time - value)/10000;
                    timer.Record(diff, TimeUnit.Milliseconds);
                });
            }

            return 0;
        }

        private void ListenToConsole(BalancedConsumer consumer)
        {
            _consoleThread = new Thread(() =>
            {
                Console.CancelKeyPress += (sender, eventArgs) =>
                {
                    Console.WriteLine("Kill!");
                    _consoleThread.Abort();
                };
                while (true)
                {
                    var input = Console.ReadKey(true);
                    if (input.KeyChar == 'q')
                    {
                        Console.WriteLine("Shutting down...");
                        consumer.Shutdown();
                        break;
                    }
                }
            });
            _consoleThread.Start();
        }
    }
}
