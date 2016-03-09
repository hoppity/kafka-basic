using System;
using System.Linq;
using System.Threading;
using Metrics;
using SimpleKafka;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var zkConnect = args.Length > 0 ? args[0] : "192.168.33.10:2181";
            var testTopic = args.Length > 1 ? args[1] : "test.topic";

            var timer = Metric.Timer("Sent", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            var client = new KafkaClient(zkConnect);
            var topic = client.Topic(testTopic);

            var stop = false;
            Console.CancelKeyPress += (sender, eventArgs) => stop = true;
            while (!stop)
            {
                Thread.Sleep(10);
                var batch =
                    Enumerable.Range(0, 100)
                        .Select(i =>
                            new Message
                            {
                                Key = i.ToString(),
                                Value = DateTime.UtcNow.Ticks.ToString()
                            })
                        .ToArray();
                var time = DateTime.UtcNow.Ticks;
                topic.Send(batch);
                var diff = (DateTime.UtcNow.Ticks - time) / 10000;
                timer.Record(diff, TimeUnit.Milliseconds);
            }
        }
    }
}
