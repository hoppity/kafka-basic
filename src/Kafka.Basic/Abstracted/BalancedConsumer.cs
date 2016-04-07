using System;
using System.Threading.Tasks;
using log4net;

namespace Kafka.Basic.Abstracted
{
    public interface IBalancedConsumer : IAbstractedConsumer<ConsumedMessage> { }

    public class BalancedConsumer : IBalancedConsumer
    {
        private static readonly object Lock = new object();

        private static readonly ILog Logger = LogManager.GetLogger(typeof(BatchedConsumer));

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
            StartAsync(dataSubscriber, errorSubscriber, closeAction).Wait();
        }

        public async Task StartAsync(
            Action<ConsumedMessage> dataSubscriber,
            Action<Exception> errorSubscriber = null,
            Action closeAction = null)
        {
            if (_running) return;

            var consumerOptions = new ConsumerOptions
            {
                GroupName = _group,
                AutoCommit = true
            };

            bool restart;
            do
            {
                lock (Lock)
                {
                    Logger.InfoFormat("Starting balanced consumer {0} for {1}.", _group, _topic);

                    var consumer = _client.Consumer(consumerOptions);

                    restart = false;

                    _instance = consumer.Join();
                    _instance.ZookeeperSessionExpired += (sender, args) =>
                    {
                        Logger.WarnFormat("Zookeeper session expired. Shutting down consumer {0} for {1} to restart...", _group, _topic);
                        restart = true;
                        Shutdown();
                    };
                    _instance.ZookeeperDisconnected += (sender, args) =>
                    {
                        Logger.WarnFormat("Zookeeper disconnected. Shutting down consumer {0} for {1} to restart...", _group, _topic);
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

                await Task.Run(() => _stream.Block());

                Logger.InfoFormat("Consumer {0} for {1} shut down.", _group, _topic);
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
