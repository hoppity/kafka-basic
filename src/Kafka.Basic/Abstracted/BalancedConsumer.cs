using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Client.Utils;
using log4net;

namespace Kafka.Basic.Abstracted
{
    public interface IBalancedConsumer : IAbstractedConsumer<ConsumedMessage> { }

    public class BalancedConsumer : IBalancedConsumer
    {
        private const int DefaultNumberOfThreads = 1;

        private static readonly object Lock = new object();

        private static readonly ILog Logger = LogManager.GetLogger(typeof(BalancedConsumer));

        private readonly IKafkaClient _client;
        private readonly string _group;
        private readonly string _topic;
        private readonly int _threads;
        private bool _running;
        private IKafkaConsumerInstance _instance;
        private IEnumerable<IKafkaConsumerStream> _streams;
        private bool _restart;

        public BalancedConsumer(
            IKafkaClient client,
            string group,
            string topic,
            int threads = DefaultNumberOfThreads)
        {
            _client = client;
            _group = group;
            _topic = topic;
            _threads = threads;
        }

        public void Start(Action<ConsumedMessage> dataSubscriber, Action<Exception> errorSubscriber = null, Action closeAction = null)
        {
            if (_running) return;

            var consumerOptions = new ConsumerOptions
            {
                GroupName = _group,
                AutoCommit = true
            };

            do
            {
                if (_restart) Task.Delay(5000).Wait();

                lock (Lock)
                {
                    try
                    {
                        Logger.Info("Starting consumer.");

                        var consumer = _client.Consumer(consumerOptions);

                        _restart = false;

                        _instance = consumer.Join();
                        _instance.ZookeeperSessionExpired += (sender, args) =>
                        {
                            Logger.Warn("Zookeeper session expired. Shutting down to restart...");
                            Restart();
                        };
                        _instance.ZookeeperDisconnected += (sender, args) =>
                        {
                            Logger.Warn("Zookeeper disconnected. Shutting down to restart...");
                            Restart();
                        };
                        _streams = _instance.Subscribe(_topic, _threads).ToArray();

                        _streams.ForEach(s =>
                        {
                            s.Data(dataSubscriber);

                            if (errorSubscriber != null) s.Error(errorSubscriber);
                            if (closeAction != null) s.Close(closeAction);

                            s.Start();
                        });
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Exception starting consumer {_group} for {_topic}. Restarting...", ex);
                        Restart();
                    }

                    _running = true;
                }

                foreach (var s in _streams) s.Block();

            } while (_restart);

            _running = false;
        }

        private void Restart()
        {
            _restart = true;
            Shutdown();
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
