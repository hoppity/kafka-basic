using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using Kafka.Client.Utils;

namespace Kafka.Basic
{
    public class KafkaBatchedConsumerStream : IKafkaBatchedConsumerStream
    {
        private readonly IKafkaConsumerInstance _instance;
        private readonly IKafkaConsumerStream _stream;
        private readonly string _topicName;
        private readonly int _batchSize;
        private readonly int _timeoutMs;

        private readonly EventWaitHandle _lockEvent;
        private Action<IEnumerable<ConsumedMessage>> _dataSubscriber;
        private Action<Exception> _errorSubscriber;
        private Action _closeSubscriber;

        public KafkaBatchedConsumerStream(IKafkaConsumerInstance instance, IKafkaConsumerStream stream, string topicName, int batchSize, int timeoutMs)
        {
            _instance = instance;
            _stream = stream;
            _topicName = topicName;
            _batchSize = batchSize;
            _timeoutMs = timeoutMs;

            _lockEvent = new EventWaitHandle(true, EventResetMode.ManualReset);
        }

        public void Dispose()
        {
            _stream?.Dispose();
        }

        public IKafkaBatchedConsumerStream Data(Action<IEnumerable<ConsumedMessage>> action)
        {
            _dataSubscriber = action;
            return this;
        }

        public IKafkaBatchedConsumerStream Error(Action<Exception> action)
        {
            _errorSubscriber = action;
            return this;
        }

        public IKafkaBatchedConsumerStream Close(Action action)
        {
            _closeSubscriber = action;
            return this;
        }

        public IKafkaBatchedConsumerStream Start()
        {
            Thread.Sleep(5000);
            var batch = new BatchBlock<ConsumedMessage>(_batchSize);
            var timer = new Timer(_ => batch.TriggerBatch(), null, _timeoutMs, Timeout.Infinite);
            var receiveTransform = new ActionBlock<ConsumedMessage[]>(messages =>
            {
                _lockEvent.Reset();
                timer.Change(Timeout.Infinite, Timeout.Infinite);
                _dataSubscriber(messages);
                messages
                    .GroupBy(m => m.Partition)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Max(m => m.Offset) + 1)
                    .ForEach(kvp => _instance.Commit(_topicName, kvp.Key, kvp.Value));
                _lockEvent.Set();
                timer.Change(_timeoutMs, Timeout.Infinite);
            });
            batch.LinkTo(receiveTransform);

            _stream
                .Data(message =>
                {
                    _lockEvent.WaitOne();
                    batch.Post(message);
                })
                .Error(_errorSubscriber)
                .Close(_closeSubscriber)
                .Start();

            return this;
        }

        public void Block()
        {
            _stream.Block();
        }

        public void Shutdown()
        {
            _stream.Shutdown();
        }

        public void Pause()
        {
            _stream.Pause();
        }

        public void Resume()
        {
            _stream.Resume();
        }
    }
}