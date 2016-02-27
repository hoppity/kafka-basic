using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Messages;
using Kafka.Client.Requests;
using Kafka.Client.Serialization;
using System.Text;
using Metrics;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var zookeeperString = args.Length > 0 ? args[0] : "192.168.33.10:2181";
            var consumerGroupId = args.Length > 1 ? args[1] : "test.group";
            var testTopic = args.Length > 2 ? args[2] : "test.topic";

            var uniqueConsumerId = consumerGroupId + "." + Guid.NewGuid().ToString("N");
            var m_BufferMaxNoOfMessages = 1000;
            var fetchSize = 11 * 1024 * 1024;

            var timer = Metric.Timer("Received", Unit.Events);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            // Here we create a balanced consumer on one consumer machine for consumerGroupId. All machines consuming for this group will get balanced together
            ConsumerConfiguration config = new ConsumerConfiguration
            {
                AutoCommit = false,
                GroupId = consumerGroupId,
                ConsumerId = uniqueConsumerId,
                MaxFetchBufferLength = m_BufferMaxNoOfMessages,
                FetchSize = fetchSize,
                AutoOffsetReset = OffsetRequest.LargestTime,
                NumberOfTries = 20,
                ZooKeeper = new ZooKeeperConfiguration(zookeeperString, 30000, 30000, 2000)
            };
            var balancedConsumer = new ZookeeperConsumerConnector(config, true, RebalanceHandler, ZkDisconnectedHandler, ZkExpiredHandler);
            // grab streams for desired topics 
            var streams = balancedConsumer.CreateMessageStreams(new ConcurrentDictionary<string, int>(new Dictionary<string, int>
            {
                { testTopic, 1 }
            }), new DefaultDecoder());
            var stream = streams[testTopic][0];
            var cancellationTokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                cancellationTokenSource.Cancel();
                balancedConsumer.Dispose();
            };
            foreach (Message message in stream.GetCancellable(cancellationTokenSource.Token))
            {
                var time = DateTime.UtcNow.Ticks;
                var text = Encoding.UTF8.GetString(message.Payload);
                var value = long.Parse(text);
                var diff = (time - value) / 10000;
                timer.Record(diff, TimeUnit.Milliseconds);
            }
        }

        private static void ZkExpiredHandler(object sender, EventArgs eventArgs)
        {
            Console.WriteLine($"{DateTime.Now.Ticks}:ZK_EXPIRE");
        }

        private static void ZkDisconnectedHandler(object sender, EventArgs eventArgs)
        {
            Console.WriteLine($"{DateTime.Now.Ticks}:ZK_DISCONNECT");
        }

        public static void RebalanceHandler(object sender, EventArgs eventArgs)
        {
            Console.WriteLine($"{DateTime.Now.Ticks}:REBALANCE");
        }
    }
}
