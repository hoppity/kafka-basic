using System;
using System.Linq;
using System.Threading;
using Kafka.Basic;
using Metrics;

namespace Consumer
{
    class BatchedConsumer
    {
        private Thread _consoleThread;

        public int Start(BatchedConsumerOptions opts)
        {
            var timer = Metric.Timer("Received", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            using (var client = new KafkaClient(opts.ZkConnect))
            using (var consumer = new Kafka.Basic.BatchedConsumer(client, opts.Group, opts.Topic, opts.BatchSizeMax, opts.BatchTimeoutMs))
            {
                ListenToConsole(consumer);
                Thread.Sleep(5000);
                consumer
                    .Start(m =>
                    {
                        var time = DateTime.UtcNow.Ticks;
                        m.ToList()
                            .ForEach(message =>
                            {
                                var value = long.Parse(message.Value);
                                var diff = (time - value) / 10000;
                                timer.Record(diff, TimeUnit.Milliseconds);
                            });
                    }
                    );
            }

            return 0;
        }

        private void ListenToConsole(Kafka.Basic.BatchedConsumer consumer)
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
                    //if (input.KeyChar == 'p')
                    //{
                    //    stream.Pause();
                    //    Console.WriteLine("Paused.");
                    //}
                    //if (input.KeyChar == 'r')
                    //{
                    //    stream.Resume();
                    //    Console.WriteLine("Resumed.");
                    //}
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
