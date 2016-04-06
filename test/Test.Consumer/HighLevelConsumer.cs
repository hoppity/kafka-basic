using System;
using System.Threading;
using Kafka.Basic;
using Metrics;

namespace Consumer
{
    class HighLevelConsumer : ConsoleApp
    {
        public int Start(HighLevelConsumerOptions opts)
        {
            var timer = Metric.Timer("Received", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            using (var client = new KafkaClient(opts.ZkConnect))
            {
                var consumerGroup = client.Consumer(opts.Group);
                using (var instance = consumerGroup.Join())
                {
                    var stream = instance.Subscribe(opts.Topic);

                    ListenToConsole(instance.Shutdown, stream.Pause, stream.Resume);

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
            }

            return 0;
        }
    }
}
