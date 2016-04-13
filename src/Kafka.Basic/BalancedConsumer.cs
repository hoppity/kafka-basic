using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Client.Utils;
using log4net;

namespace Kafka.Basic
{
    public interface IBalancedConsumer : IConsumer<ConsumedMessage> { }

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
                    Logger.Info($"Starting balanced consumer {_group} for {_topic}.");
                    _restart = false;

                    try
                    {
                        var consumer = _client.Consumer(consumerOptions);

                        _instance = consumer.Join();
                        _instance.ZookeeperSessionExpired += (sender, args) =>
                        {
                            Logger.WarnFormat($"Zookeeper session expired. Shutting down consumer {_group} for {_topic} to restart...");
                            Restart();
                        };
                        _instance.ZookeeperDisconnected += (sender, args) =>
                        {
                            Logger.WarnFormat($"Zookeeper disconnected. Shutting down consumer {_group} for {_topic} to restart...");
                            Restart();
                        };
                        _streams = _instance.Subscribe(_topic, _threads).ToArray();

                        _streams.ForEach(s =>
                        {
                            s.Data(m =>
                            {
                                try
                                {
                                    dataSubscriber(m);
                                }
                                catch (Exception ex)
                                {
                                    Logger.Error($"Exception in consumer {_group} for {_topic}. Restarting...", ex);
                                    errorSubscriber?.Invoke(ex);

                                    Restart();
                                }
                            });

                            if (errorSubscriber != null) s.Error(errorSubscriber);

                            s.Start();
                        });

                        Logger.Info($"Consumer {_instance.Id} started with {_streams.Count()} threads.");

                        _running = true;
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Exception starting consumer {_group} for {_topic}. Restarting...", ex);
                        Restart();
                        continue;
                    }
                }

                foreach (var s in _streams) s.Block();

                Logger.Info($"Consumer {_group} for {_topic} shut down.");
            } while (_restart);

            closeAction?.Invoke();
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
