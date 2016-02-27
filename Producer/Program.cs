using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Client.Cfg;
using Kafka.Client.Messages;
using Kafka.Client.Producers;
using Metrics;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var brokerId = 0;
            var kafkaServerName = "192.168.33.10";
            var kafkaPort = 9092;
            var testTopic = "test.topic.1";

            var timer = Metric.Timer("Sent", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            var brokerConfig = new BrokerConfiguration()
            {
                BrokerId = brokerId,
                Host = kafkaServerName,
                Port = kafkaPort
            };
            var config = new ProducerConfiguration(new List<BrokerConfiguration> { brokerConfig });
            var kafkaProducer = new Kafka.Client.Producers.Producer(config);
            var stop = false;
            Console.CancelKeyPress += (sender, eventArgs) => stop = true;
            while (!stop)
            {
                Thread.Sleep(10);
                var batch =
                    Enumerable.Range(0, 100)
                        .Select(i =>
                            new ProducerData<string, Message>(testTopic, new[]
                            {
                                new Message(
                                    Encoding.UTF8.GetBytes(DateTime.UtcNow.Ticks.ToString()),
                                    Encoding.UTF8.GetBytes(i.ToString()),
                                    CompressionCodecs.NoCompressionCodec
                                    )
                            })
                        );
                var time = DateTime.UtcNow.Ticks;
                kafkaProducer.Send(batch);
                var diff = (DateTime.UtcNow.Ticks - time) / 10000;
                timer.Record(diff, TimeUnit.Milliseconds);
            }
        }
    }
}
