using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace Kafka.Basic.Abstracted
{
    public interface IBalancedConsumer : IAbstractedConsumer<ConsumedMessage> { }
    public class BalancedConsumer : IBalancedConsumer
    {
        private static readonly object Lock = new object();

        private readonly IKafkaClient _client;
        private readonly string _group;
        private readonly string _topic;
        private bool _running;
        private IKafkaConsumerInstance _instance;
        private IKafkaConsumerStream _stream;

        public BalancedConsumer(
            IKafkaClient client,
            string group,
            string topic)
        {
            _client = client;
            _group = group;
            _topic = topic;
        }

        public void Start(Action<ConsumedMessage> dataSubscriber, Action<Exception> errorSubscriber = null, Action closeAction = null)
        {
            if (_running) return;

            var consumerOptions = new ConsumerOptions
            {
                GroupName = _group,
                AutoCommit = false
            };

            bool restart;
            BatchBlock<ConsumedMessage> batchBlock;
            Timer timer;
            do
            {
                lock (Lock)
                {
                    Console.WriteLine("Starting consumer.");

                    var consumer = _client.Consumer(consumerOptions);

                    restart = false;

                    _instance = consumer.Join();
                    _instance.ZookeeperSessionExpired += (sender, args) =>
                    {
                        Console.WriteLine("Zookeeper session expired. Shutting down to restart...");
                        restart = true;
                        Shutdown();
                    };
                    _instance.ZookeeperDisconnected += (sender, args) =>
                    {
                        Console.WriteLine("Zookeeper disconnected. Shutting down to restart...");
                        restart = true;
                        Shutdown();
                    };
                    _stream = _instance.Subscribe(_topic);

                    _stream.Data(m => { lock (Lock) dataSubscriber(m); });

                    if (errorSubscriber != null) _stream.Error(errorSubscriber);
                    if (closeAction != null) _stream.Close(closeAction);

                    _stream.Start();

                    _running = true;
                }

                _stream.Block();

            } while (restart);

            _running = false;
        }
        
        public void Shutdown()
        {
            if (!_running) return;
            lock (Lock)
            {
                if (!_running) return;

                _instance?.Shutdown();
                _instance?.Dispose();
                _instance = null;
            }
        }

        public void Dispose()
        {
            Shutdown();
        }
    }
}
