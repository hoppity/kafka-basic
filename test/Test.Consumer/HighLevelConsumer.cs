using System;
using System.Threading;
using Kafka.Basic;
using Metrics;

namespace Consumer
{
    class HighLevelConsumer
    {
        public int Start(HighLevelConsumerOptions opts)
        {
            var zookeeperString = opts.ZkConnect;
            var consumerGroupId = opts.Group;
            var testTopic = opts.Topic;
            
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

            return 0;
        }
    }
}
