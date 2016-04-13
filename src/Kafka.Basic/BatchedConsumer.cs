using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Client.Consumers;
using Kafka.Client.Serialization;
using log4net;

namespace Kafka.Basic
{
    public interface IBatchedConsumer : IConsumer<IEnumerable<ConsumedMessage>> { }

    public class BatchedConsumer : IBatchedConsumer
    {
        public const int DefaultNumberOfThreads = 1;
        public const int DefaultBatchTimeoutMs = 100;

        private static readonly object Lock = new object();

        private static readonly ILog Logger = LogManager.GetLogger(typeof(BatchedConsumer));

        private readonly ZookeeperConnection _connection;
        private readonly string _group;
        private readonly string _topic;
        private readonly int _threads;
        private readonly int _batchTimeoutMs;

        private bool _running;
        private bool _forceShutdown;
        private bool _restart;
        private IConsumerConnector _consumer;

        public BatchedConsumer(
            string connection,
            string group,
            string topic,
            int threads = DefaultNumberOfThreads,
            int batchTimeoutMs = DefaultBatchTimeoutMs)
        {
            _connection = new ZookeeperConnection(connection);
            _group = group;
            _topic = topic;
            _threads = threads;
            _batchTimeoutMs = batchTimeoutMs;
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
                if (_restart) Task.Delay(5000).Wait();

                if (_forceShutdown) break;

                Task[] tasks;
                lock (Lock)
                {
                    Logger.InfoFormat("Starting batched consumer {0} for {1}.", _group, _topic);

                    _restart = false;

                    try
                    {
                        _consumer = _connection.CreateConsumerConnector(new ConsumerOptions
                        {
                            GroupName = _group,
                            AutoCommit = false,
                            AutoOffsetReset = Offset.Earliest
                        });

                        var streams = _consumer.CreateMessageStreams(
                            new Dictionary<string, int>
                            {
                            {_topic, _threads}
                            },
                            new DefaultDecoder());

                        _consumer.ZookeeperSessionExpired += (sender, args) =>
                        {
                            Logger.WarnFormat("Zookeeper session expired. Shutting down consumer {0} for {1} to restart...", _group, _topic);
                            Restart();
                        };
                        _consumer.ZookeeperDisconnected += (sender, args) =>
                        {
                            Logger.WarnFormat("Zookeeper disconnected. Shutting down consumer {0} for {1} to restart...", _group, _topic);
                            Restart();
                        };

                        _running = true;

                        tasks = streams[_topic]
                            .Select(s => StartConsumer(s, dataSubscriber, errorSubscriber))
                            .ToArray();
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

                        if (messages.Length == 0) continue;

                        dataSubscriber(messages);

                        var map = messages
                            .GroupBy(m => m.Partition)
                            .ToDictionary(kvp => kvp.Key, kvp => kvp.Max(m => m.Offset + 1));

                        foreach (var kvp in map)
                            Commit(kvp.Key, kvp.Value);
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
    }
}
