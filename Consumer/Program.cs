using System;
using System.Threading;
using Metrics;
using SimpleKafka;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var zookeeperString = args.Length > 0 ? args[0] : "192.168.33.10:2181";
            var consumerGroupId = args.Length > 1 ? args[1] : "test.group";
            var testTopic = args.Length > 2 ? args[2] : "test.topic";
            
            var timer = Metric.Timer("Received", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            var handler = new AutoResetEvent(false);
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
                handler.Set();
            };

            using (var client = new KafkaClient(zookeeperString))
            {
                var consumerGroup = client.Consumer(consumerGroupId);
                using (var instance = consumerGroup.Join())
                {
                    instance.Subscribe(testTopic)
                        .Data(message =>
                        {
                            var time = DateTime.UtcNow.Ticks;
                            var value = long.Parse(message.Value);
                            var diff = (time - value) / 10000;
                            timer.Record(diff, TimeUnit.Milliseconds);
                        })
                        .Start();
                    handler.WaitOne();
                }
            }
        }
    }
}
