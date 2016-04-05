using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace Kafka.Basic
{
    public interface IBatchedConsumer
    {
        void Start(
            Action<IEnumerable<ConsumedMessage>> dataSubscriber,
            Action<Exception> errorSubscriber = null,
            Action closeAction = null);

        void Shutdown();
    }

    public class BatchedConsumer : IDisposable, IBatchedConsumer
    {
        public const int DefaultBatchSizeMax = 1000;
        public const int DefaultBatchTimeoutMs = 100;

        private static readonly object Lock = new object();

        private readonly IKafkaClient _client;
        private readonly string _group;
        private readonly string _topic;
        private readonly int _batchSizeMax;
        private readonly int _batchTimeoutMs;

        private bool _running;
        private IKafkaConsumerInstance _instance;
        private IKafkaConsumerStream _stream;

        public BatchedConsumer(
            IKafkaClient client,
            string group,
            string topic,
            int batchSizeMax = DefaultBatchSizeMax,
            int batchTimeoutMs = DefaultBatchTimeoutMs)
        {
            _client = client;
            _group = group;
            _topic = topic;
            _batchSizeMax = batchSizeMax;
            _batchTimeoutMs = batchTimeoutMs;
        }


        public void Start(
            Action<IEnumerable<ConsumedMessage>> dataSubscriber,
            Action<Exception> errorSubscriber = null,
            Action closeAction = null)
        {
            if (!_running)
            {
                lock (Lock)
                {
                    if (_running) return;

                    _running = true;
                }
            }
            var consumerOptions = new ConsumerOptions
            {
                GroupName = _group,
                AutoCommit = false
            };

            var consumer = _client.Consumer(consumerOptions);

            _instance = consumer.Join();
            _stream = _instance.Subscribe(_topic);

            var batchBlock = new BatchBlock<ConsumedMessage>(_batchSizeMax);
            using (var timer = new Timer(_ => batchBlock.TriggerBatch(), null, _batchTimeoutMs, _batchTimeoutMs))
            {
                var actionBlock = new ActionBlock<ConsumedMessage[]>(messages =>
                {
                    lock (Lock)
                    {
                        timer.Change(Timeout.Infinite, Timeout.Infinite);
                        dataSubscriber(messages);

                        var map = messages
                            .GroupBy(m => m.Partition)
                            .ToDictionary(kvp => kvp.Key, kvp => kvp.Max(m => m.Offset));

                        foreach (var kvp in map)
                            _instance.Commit(_topic, kvp.Key, kvp.Value);
                    }
                    timer.Change(_batchTimeoutMs, _batchTimeoutMs);
                });
                batchBlock.LinkTo(actionBlock);

                _stream.Data(m => { lock (Lock) batchBlock.Post(m); });

                if (errorSubscriber != null) _stream.Error(errorSubscriber);
                if (closeAction != null) _stream.Close(closeAction);

                _stream.Start()
                    .Block();

                batchBlock.Complete();
            }
        }

        public void Shutdown()
        {
            if (!_running) return;
            lock (Lock)
            {
                if (!_running) return;

                _instance?.Shutdown();
            }
        }

        public void Dispose()
        {
            Shutdown();

            _instance?.Dispose();
        }
    }
}
