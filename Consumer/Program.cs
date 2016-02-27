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

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var zookeeperString = "192.168.33.10:2181";
            var consumerGroupId = "test.group";
            var uniqueConsumerId = "test.group." + Guid.NewGuid().ToString("N");
            var m_BufferMaxNoOfMessages = 1000;
            var fetchSize = 11 * 1024 * 1024;

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
                { "test.topic", 1 }
            }), new DefaultDecoder());
            var stream = streams["test.topic"][0];
            var cancellationTokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) => cancellationTokenSource.Cancel();
            foreach (Message message in stream.GetCancellable(cancellationTokenSource.Token))
            {
                Console.WriteLine($"{message.PartitionId}-{message.Offset}:{Encoding.UTF8.GetString(message.Key)}-{Encoding.UTF8.GetString(message.Payload)}");
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
