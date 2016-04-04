using System;
using System.Linq;
using System.Threading;
using Kafka.Basic;
using Metrics;
using Timer = Metrics.Timer;

namespace Consumer
{
    class HighLevelConsumer
    {
        private Thread _consoleThread;

        public int Start(HighLevelConsumerOptions opts)
        {
            var zookeeperString = opts.ZkConnect;
            var consumerGroupId = opts.Group;

            var timer = Metric.Timer("Received", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            using (var client = new KafkaClient(zookeeperString))
            {
                var consumerGroup = client.Consumer(consumerGroupId);
                using (var instance = consumerGroup.Join())
                {
                    if (opts.Batch)
                        StartBatched(instance, opts, timer);
                    else
                        StartNonBatched(instance, opts, timer);
                }
            }

            return 0;
        }

        private void StartBatched(IKafkaConsumerInstance instance, HighLevelConsumerOptions opts, Timer timer)
        {
            var stream = instance.Subscribe(opts.Topic, opts.BatchSize, opts.BatchTimeoutMs);

            ListenToConsole(instance, stream);

            stream
                .Data(messages =>
                {
                    messages.ToList()
                        .ForEach(message =>
                        {
                            var time = DateTime.UtcNow.Ticks;
                            var value = long.Parse(message.Value);
                            var diff = (time - value) / 10000;
                            timer.Record(diff, TimeUnit.Milliseconds);
                        });
                })
                .Start()
                .Block();
        }

        private void StartNonBatched(IKafkaConsumerInstance instance, HighLevelConsumerOptions opts, Timer timer)
        {
            var stream = instance.Subscribe(opts.Topic);

            ListenToConsole(instance, stream);

            stream
                .Data(message =>
                {
                    var time = DateTime.UtcNow.Ticks;
                    var value = long.Parse(message.Value);
                    var diff = (time - value) / 10000;
                    timer.Record(diff, TimeUnit.Milliseconds);
                })
                .Start()
                .Block();
        }

        private void ListenToConsole(IKafkaConsumerInstance instance, IKafkaStream stream)
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
                    if (input.KeyChar == 'p')
                    {
                        stream.Pause();
                        Console.WriteLine("Paused.");
                    }
                    if (input.KeyChar == 'r')
                    {
                        stream.Resume();
                        Console.WriteLine("Resumed.");
                    }
                    if (input.KeyChar == 'q')
                    {
                        Console.WriteLine("Shutting down...");
                        instance.Shutdown();
                        break;
                    }
                }
            });
            _consoleThread.Start();
        }
    }
}
