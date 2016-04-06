using System;
using System.Linq;
using Kafka.Basic;
using Metrics;

namespace Consumer
{
    class BatchedConsumer : ConsoleApp
    {
        public int Start(BatchedConsumerOptions opts)
        {
            var histogram = Metric.Histogram("batch.size", Unit.Items);
            var timer = Metric.Timer("message.latency", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            using (var client = new KafkaClient(opts.ZkConnect))
            using (var consumer = new Kafka.Basic.Abstracted.BatchedConsumer(client, opts.Group, opts.Topic, opts.BatchSizeMax, opts.BatchTimeoutMs))
            {
                ListenToConsole(() => consumer.Shutdown());

                consumer
                    .Start(m =>
                    {
                        var time = DateTime.UtcNow.Ticks;
                        var list = m.ToList();
                        list
                            .ForEach(message =>
                            {
                                var value = long.Parse(message.Value);
                                var diff = (time - value) / 10000;
                                timer.Record(diff, TimeUnit.Milliseconds);
                            });
                        histogram.Update(list.Count);
                    });
            }

            return 0;
        }
    }
}
