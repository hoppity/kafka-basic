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
            
            using (var client = new KafkaClient(zookeeperString))
            {
                var consumerGroup = client.Consumer(consumerGroupId);
                using (var instance = consumerGroup.Join())
                {
                    Console.CancelKeyPress += (sender, eventArgs) =>
                    {
                        eventArgs.Cancel = true;
                        instance.Shutdown();
                    };
                    instance.Subscribe(testTopic)
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
            }

            return 0;
        }
    }
}
