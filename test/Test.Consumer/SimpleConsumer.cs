using System;
using System.Collections.Generic;
using System.Threading;
using Kafka.Basic;
using Metrics;

namespace Consumer
{
    class SimpleConsumer
    {
        private Thread _consoleThread;

        public int Start(SimpleConsumerOptions opts)
        {
            var zookeeperString = opts.ZkConnect;
            var testTopic = opts.Topic;
            var partition = opts.Partition;

            var partitions = partition == null
                ? null
                : new[] { partition.Value };

            var timer = Metric.Timer("Received", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            using (var client = new KafkaClient(zookeeperString))
            using (var consumer = new Kafka.Basic.SimpleConsumer(client, testTopic, partitions))
            {
                ListenToConsole(consumer);

                consumer.Start(
                    message =>
                    {
                        var time = DateTime.UtcNow.Ticks;
                        var value = long.Parse(message.Value);
                        var diff = (time - value) / 10000;
                        timer.Record(diff, TimeUnit.Milliseconds);
                    },
                    e => Console.Error.WriteLine(e.Message)
                );
            }

            return 0;
        }

        private void ListenToConsole(Kafka.Basic.SimpleConsumer stream)
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
                        stream.Shutdown();
                        break;
                    }
                }
            });
            _consoleThread.Start();
        }
    }
}
