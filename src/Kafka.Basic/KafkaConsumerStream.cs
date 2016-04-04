using System;
using System.Text;
using System.Threading;
using Kafka.Client.Consumers;

namespace Kafka.Basic
{
    public class KafkaConsumerStream : IKafkaConsumerStream
    {
        private readonly IKafkaMessageStream<Client.Messages.Message> _stream;
        private readonly CancellationTokenSource _tokenSource;
        private readonly Thread _thread;
        private bool _running;
        private AutoResetEvent _shutdownEvent;
        private EventWaitHandle _resumeEvent;

        private Action<ConsumedMessage> _dataSubscriber;
        private Action<Exception> _errorSubscriber;
        private Action _closeSubscriber;

        public KafkaConsumerStream(IKafkaMessageStream<Client.Messages.Message> stream)
        {
            _tokenSource = new CancellationTokenSource();
            _stream = stream.GetCancellable(_tokenSource.Token);
            _thread = new Thread(RunConsumer);
        }

        public IKafkaConsumerStream Data(Action<ConsumedMessage> action)
        {
            _dataSubscriber = action;
            return this;
        }

        public IKafkaConsumerStream Error(Action<Exception> action)
        {
            _errorSubscriber = action;
            return this;
        }

        public IKafkaConsumerStream Close(Action action)
        {
            _closeSubscriber = action;
            return this;
        }

        public IKafkaConsumerStream Start()
        {
            _shutdownEvent = new AutoResetEvent(false);
            _resumeEvent = new EventWaitHandle(true, EventResetMode.ManualReset);
            _running = true;
            _thread.Start();
            return this;
        }

        public void Block()
        {
            _shutdownEvent.WaitOne();
        }

        public void Pause()
        {
            _resumeEvent.Reset();
        }

        public void Resume()
        {
            _resumeEvent.Set();
        }

        private void RunConsumer()
        {
            while (_running)
            {
                if (_dataSubscriber == null) continue;

                try
                {
                    _resumeEvent.WaitOne();

                    if (!_stream.iterator.MoveNext()) continue;
                    var message = _stream.iterator.Current;

                    _dataSubscriber(new ConsumedMessage
                    {
                        Partition = message.PartitionId ?? 0,
                        Offset = message.Offset,
                        Key = message.Key == null ? null : Encoding.UTF8.GetString(message.Key),
                        Value = message.Payload == null ? null : Encoding.UTF8.GetString(message.Payload)
                    });
                }
                catch (Exception ex)
                {
                    _errorSubscriber?.Invoke(ex);
                }
            }
            _closeSubscriber?.Invoke();
        }

        public void Shutdown()
        {
            _running = false;
            _tokenSource.Cancel();
            _shutdownEvent.Set();
        }

        public void Dispose()
        {
            if (_running) Shutdown();

            _tokenSource?.Dispose();

            _dataSubscriber = null;
            _errorSubscriber = null;
            _closeSubscriber = null;
        }
    }
}