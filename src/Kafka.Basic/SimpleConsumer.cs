using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Client.Utils;
using log4net;

namespace Kafka.Basic
{
    public interface ISimpleConsumer : IConsumer<ConsumedMessage> { }

    public class SimpleConsumer : ISimpleConsumer
    {
        private static readonly object Lock = new object();
        private static readonly ILog Logger = LogManager.GetLogger(typeof(BalancedConsumer));

        private readonly IKafkaClient _client;
        private readonly string _topic;

        private int[] _partitions;
        private IKafkaSimpleConsumer _consumer;
        private IEnumerable<IKafkaConsumerStream> _streams;

        private bool _running;
        private bool _restart;
        private bool _forceShutdown;

        public SimpleConsumer(IKafkaClient client, string topic, int[] partitions = null)
        {
            _client = client;
            _topic = topic;
            _partitions = partitions;
        }

        public void Start(Action<ConsumedMessage> dataSubscriber, Action<Exception> errorSubscriber = null, Action closeAction = null)
        {
            if (_running) return;

            do
            {
                if (_restart) Task.Delay(5000).Wait();

                lock (Lock)
                {
                    _restart = false;

                    try
                    {
                        Logger.Info($"Starting simple consumer for {_topic}.");

                        _consumer = _client.SimpleConsumer();

                        if (_partitions == null)
                        {
                            Logger.Info("Determining partitions from metadata.");
                            var meta = _client.Topic(_topic).GetMetadata();
                            _partitions = meta.PartitionsMetadata.Select(p => p.PartitionId).ToArray();
                        }

                        Logger.Info($"Subscribing to {_topic} partitions {string.Join(",", _partitions)}");

                        _streams = _partitions
                            .Select(p => _consumer.Subscribe(_topic, p, (long)Offset.Latest))
                            .ToArray();

                        foreach (var stream in _streams)
                        {
                            stream.Data(dataSubscriber);
                            if (errorSubscriber != null) stream.Error(errorSubscriber);
                            stream.Start();
                        }

                        Logger.Info($"Simple consumer started with {_streams.Count()} threads.");
                        _running = true;
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Exception starting simple consumer for {_topic}. Restarting...", ex);
                        Restart();
                        continue;
                    }
                }

                _streams?.ForEach(s => s.Block());

                Logger.Info($"Simple consumer for {_topic} shut down.");
            } while (_restart);

            closeAction?.Invoke();
            _running = false;
        }

        private void Restart()
        {
            _restart = true;
            ShutdownInternal();
        }

        public void Shutdown()
        {
            ShutdownInternal(true);
        }

        private void ShutdownInternal(bool force = false)
        {
            Logger.Info($"Shutdown received for simple consumer on {_topic} with force:{force}");
            if (!_running) return;
            lock (Lock)
            {
                if (!_running) return;

                if (_forceShutdown) return;
                _forceShutdown = force;

                var c = _consumer;
                _consumer = null;
                c?.Dispose();
            }
        }

        public void Dispose()
        {
            Shutdown();
        }
    }
}
