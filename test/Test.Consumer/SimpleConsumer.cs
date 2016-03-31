using System;
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
            var offset = opts.Offset;
            
            var timer = Metric.Timer("Received", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            using (var client = new KafkaClient(zookeeperString))
            using (var consumer = client.SimpleConsumer())
            using (var stream = consumer.Subscribe(testTopic, partition, offset))
            {
                ListenToConsole(stream);

                stream
                    .Data(message =>
                    {
                        var time = DateTime.UtcNow.Ticks;
                        var value = long.Parse(message.Value);
                        var diff = (time - value) / 10000;
                        timer.Record(diff, TimeUnit.Milliseconds);
                    })
                    .Error(e => Console.Error.WriteLine(e.Message))
                    .Start()
                    .Block();
            }

            return 0;
        }

        private void ListenToConsole(IKafkaConsumerStream stream)
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
                        stream.Shutdown();
                        break;
                    }
                }
            });
            _consoleThread.Start();
        }
    }
}
