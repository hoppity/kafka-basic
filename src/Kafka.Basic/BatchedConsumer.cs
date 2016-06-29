using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Client.Consumers;
using Kafka.Client.Serialization;
using Kafka.Client.ZooKeeperIntegration.Events;
using log4net;

namespace Kafka.Basic
{
    public interface IBatchedConsumer : IConsumer<IEnumerable<ConsumedMessage>>
    {
        event EventHandler<ConsumerRebalanceEventArgs> Rebalanced;
        event EventHandler ZookeeperDisconnected;
        event EventHandler ZookeeperSessionExpired;
    }

    public class BatchedConsumer : IBatchedConsumer
    {
        public const int DefaultNumberOfThreads = 1;
        public const int DefaultBatchTimeoutMs = 100;
        public const int DefaultMaxBatchSize = 1000;
        private const int RestartMsDelay = 5000;

        private static readonly object Lock = new object();
        private static readonly ILog Logger = LogManager.GetLogger(typeof(BatchedConsumer));

        public event EventHandler<ConsumerRebalanceEventArgs> Rebalanced;
        public event EventHandler ZookeeperDisconnected;
        public event EventHandler ZookeeperSessionExpired;

        private readonly ZookeeperConnection _connection;
        private readonly string _group;
        private readonly string _topic;
        private readonly int _threads;
        private readonly int _batchTimeoutMs;
        private readonly int _maxBatchSize;

        private bool _running;
        private bool _forceShutdown;
        private bool _restart;
        private IConsumerConnector _consumer;

        public BatchedConsumer(
            string connection,
            string group,
            string topic,
            int threads = DefaultNumberOfThreads,
            int batchTimeoutMs = DefaultBatchTimeoutMs,
            int maxBatchSize = DefaultMaxBatchSize)
        {
            _connection = new ZookeeperConnection(connection);
            _group = group;
            _topic = topic;
            _threads = threads;
            _batchTimeoutMs = batchTimeoutMs;
            _maxBatchSize = maxBatchSize;
        }

        public void Start(
            Action<IEnumerable<ConsumedMessage>> dataSubscriber,
            Action<Exception> errorSubscriber = null,
            Action closeAction = null)
        {
            if (_running) return;

            _restart = false;
            do
            {
                if (_restart) Task.Delay(RestartMsDelay).Wait();

                if (_forceShutdown) break;

                Task[] tasks;
                lock (Lock)
                {
                    Logger.Info($"Starting batched consumer {_group} for {_topic}.");
                    _restart = false;

                    try
                    {
                        _consumer = _connection.CreateConsumerConnector(new ConsumerOptions
                        {
                            GroupName = _group,
                            AutoCommit = false,
                            AutoOffsetReset = Offset.Earliest
                        });

                        _consumer.ZookeeperSessionExpired += (sender, args) =>
                        {
                            OnZookeeperSessionExpired();
                            Logger.WarnFormat($"Zookeeper session expired. Shutting down consumer {_group} for {_topic} to restart...");
                            Restart();
                        };
                        _consumer.ZookeeperDisconnected += (sender, args) =>
                        {
                            OnZookeeperDisconnected();
                            Logger.WarnFormat($"Zookeeper disconnected. Shutting down consumer {_group} for {_topic} to restart...");
                            Restart();
                        };
                        _consumer.Rebalanced += (sender, args) => OnRebalanced(args);

                        var streams = _consumer.CreateMessageStreams(
                            new Dictionary<string, int> { { _topic, _threads } },
                            new DefaultDecoder());

                        tasks = streams[_topic]
                            .Select(s => StartConsumer(s, dataSubscriber, errorSubscriber))
                            .ToArray();

                        Logger.Info($"Consumer {_consumer.ConsumerId} started with {tasks.Length} threads.");

                        _running = true;
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Exception starting consumer {_group} for {_topic}. Restarting...", ex);
                        Restart();
                        continue;
                    }
                }

                Task.WaitAll(tasks);

                Logger.InfoFormat("Consumer {0} for {1} shut down.", _group, _topic);
            } while (_restart);

            closeAction?.Invoke();
            _running = false;
        }

        private readonly IList<Thread> _threadList = new List<Thread>();
        private Task StartConsumer(
            IKafkaMessageStream<Client.Messages.Message> stream,
            Action<IEnumerable<ConsumedMessage>> dataSubscriber,
            Action<Exception> errorSubscriber)
        {
            var completionSource = new TaskCompletionSource<bool>();
            var thread = new Thread(() =>
            {
                Logger.Info($"Starting consumer thread {Thread.CurrentThread.ManagedThreadId}");
                while (_consumer != null)
                {
                    var tokenSource = new CancellationTokenSource(_batchTimeoutMs);
                    try
                    {
                        var cancellable = stream.GetCancellable(tokenSource.Token);
                        var messages = cancellable
                            .Select(message => message.AsConsumedMessage())
                            .ToArray();

                        Logger.Debug($"Received {messages.Length} messages.");
                        if (messages.Length == 0) continue;

                        for (var i = 0; i < messages.Length; i += _maxBatchSize)
                        {
                            if (_consumer == null) break;

                            var taken = messages.Skip(i).Take(_maxBatchSize).ToArray();
                            Logger.Debug($"Skip {i}, take {_maxBatchSize}, get {taken.Length}.");

                            dataSubscriber(taken);

                            var map = taken
                                .GroupBy(m => m.Partition)
                                .ToDictionary(kvp => kvp.Key, kvp => kvp.Max(m => m.Offset + 1));

                            foreach (var kvp in map)
                                Commit(kvp.Key, kvp.Value);
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Exception in consumer {_group} for {_topic}. Restarting...", ex);
                        errorSubscriber?.Invoke(ex);

                        Restart();
                        break;
                    }
                    finally
                    {
                        tokenSource.Dispose();
                    }
                }
                Logger.Info($"Ending consumer thread {Thread.CurrentThread.ManagedThreadId}");
                completionSource.SetResult(true);
            });

            _threadList.Add(thread);
            thread.Start();

            return completionSource.Task;
        }

        private void Commit(int partition, long offset)
        {
            lock (Lock)
            {
                _consumer?.CommitOffset(_topic, partition, offset, false);
            }
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
            Logger.Info($"Shutdown received for consumer {_group} on {_topic} with force:{force}");
            if (!_running) return;
            lock (Lock)
            {
                if (!_running) return;

                if (_forceShutdown) return;
                _forceShutdown = force;

                var c = _consumer;
                _consumer = null;
                c?.Dispose();
                _threadList.Clear();
            }
        }

        public void Dispose()
        {
            Shutdown();
        }

        protected virtual void OnRebalanced(ConsumerRebalanceEventArgs e)
        {
            Rebalanced?.Invoke(this, e);
        }

        protected virtual void OnZookeeperDisconnected()
        {
            ZookeeperDisconnected?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void OnZookeeperSessionExpired()
        {
            ZookeeperSessionExpired?.Invoke(this, EventArgs.Empty);
        }
    }
}
