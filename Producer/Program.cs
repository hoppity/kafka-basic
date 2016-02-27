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

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var brokerId = 0;
            var kafkaServerName = "192.168.33.10";
            var kafkaPort = 9092;

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
                Thread.Sleep(1000);
                var batch = new[]
                {
                    new ProducerData<string, Message>("test.topic", new[]
                    {
                        new Message(
                            Encoding.UTF8.GetBytes(DateTime.UtcNow.Ticks.ToString()),
                            Encoding.UTF8.GetBytes("1"),
                            CompressionCodecs.NoCompressionCodec
                            )
                    }),
                };
                var time = DateTime.UtcNow.Ticks;
                kafkaProducer.Send(batch);
                var taken = DateTime.UtcNow.Ticks - time;
                Console.WriteLine($"{DateTime.Now.Ticks}:Sent in {taken}t");
            }
        }
    }
}
