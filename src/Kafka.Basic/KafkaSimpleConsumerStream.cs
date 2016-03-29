using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Helper;
using Kafka.Client.Messages;
using Kafka.Client.Utils;

namespace Kafka.Basic
{
    public class KafkaSimpleConsumerStream : IKafkaConsumerStream
    {
        private const string ClientId = "KafkaNetClient";
        private const short VersionId = 0;

        private readonly string _topicName;
        private readonly int _partition;

        private readonly Thread _thread;
        private bool _running;
        private int _correlationId;

        private KafkaSimpleManager<string, Message> _manager;
        private long _nextOffset;
        private Consumer _consumer;
        private AutoResetEvent _handler;

        private Action<Message> _dataSubscriber;
        private Action<Exception> _errorSubscriber;
        private Action _closeSubscriber;

        public KafkaSimpleConsumerStream(IZookeeperConnection zkConnect, string topicName, int partition, long offset)
        {
            _topicName = topicName;
            _partition = partition;
            _manager = zkConnect.CreateSimpleManager();

            _manager.RefreshMetadata(0, ClientId, _correlationId++, _topicName, true);

            _consumer = _manager.GetConsumer(topicName, partition);

            _thread = new Thread(RunConsumer);

            _nextOffset = offset;
        }

        public IKafkaConsumerStream Data(Action<Message> action)
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
            _handler = new AutoResetEvent(false);
            _running = true;
            _thread.Start();
            return this;
        }

        public void Block()
        {
            _handler.WaitOne();
        }

        private void RunConsumer()
        {
            while (_running)
            {
                if (_dataSubscriber == null) continue;

                IEnumerable<MessageAndOffset> messageAndOffsets = null;
                try
                {
                    messageAndOffsets = Fetch();
                }
                catch (Exception ex)
                {
                    _errorSubscriber?.Invoke(ex);
                }

                if (messageAndOffsets == null)
                {
                    Thread.Sleep(1000);
                    continue;
                }


                foreach (var mo in messageAndOffsets)
                {
                    try
                    {
                        _dataSubscriber(new Message
                        {
                            Key = mo.Message.Key == null ? null : Encoding.UTF8.GetString(mo.Message.Key),
                            Value = mo.Message.Payload == null ? null : Encoding.UTF8.GetString(mo.Message.Payload)
                        });
                    }
                    catch (Exception ex)
                    {
                        _errorSubscriber?.Invoke(ex);
                    }
                    _nextOffset = mo.MessageOffset + 1;
                }
            }
            _closeSubscriber?.Invoke();
        }

        private void GetNextOffset()
        {
            long earliest, latest;
            _manager.RefreshAndGetOffset(
                VersionId,
                ClientId,
                _correlationId++,
                _topicName,
                _partition,
                true,
                out earliest,
                out latest);

            switch (_nextOffset)
            {
                case (long)Offset.Earliest:
                    _nextOffset = earliest;
                    return;
                case (long)Offset.Latest:
                    _nextOffset = latest;
                    return;
            }

            _nextOffset = Math.Max(_nextOffset, earliest);
        }

        private IEnumerable<MessageAndOffset> Fetch()
        {
            var success = false;
            var retryCount = 0;
            const int maxRetry = 3;

            while (!success && retryCount < maxRetry)
            {
                try
                {
                    var response = _consumer.Fetch(ClientId,
                        _topicName,
                        _correlationId++,
                        _partition,
                        _nextOffset,
                        ConsumerConfiguration.DefaultFetchSize,
                        ConsumerConfiguration.DefaultReceiveTimeout,
                        0);

                    if (response == null)
                    {
                        throw new KeyNotFoundException($"FetchRequest returned null response,fetchOffset={_nextOffset},leader={_consumer.Config.Broker},topic={_topicName},partition={_partition}");
                    }

                    var partitionData = response.PartitionData(_topicName, _partition);
                    if (partitionData == null)
                    {
                        throw new KeyNotFoundException($"PartitionData is null,fetchOffset={_nextOffset},leader={_consumer.Config.Broker},topic={_topicName},partition={_partition}");
                    }

                    if (partitionData.Error == ErrorMapping.OffsetOutOfRangeCode)
                    {
                        var error = $"PullMessage OffsetOutOfRangeCode,change to Latest,topic={_topicName},leader={_consumer.Config.Broker},partition={_partition},FetchOffset={_nextOffset},retryCount={retryCount},maxRetry={maxRetry}";
                        GetNextOffset();
                        return null;
                    }

                    if (partitionData.Error != ErrorMapping.NoError)
                    {
                        var error = $"PullMessage ErrorCode={partitionData.Error},topic={_topicName},leader={_consumer.Config.Broker},partition={_partition},FetchOffset={_nextOffset},retryCount={retryCount},maxRetry={maxRetry}";
                        GetNextOffset();
                        return null;
                    }

                    success = true;
                    var messages = partitionData.GetMessageAndOffsets();
                    if (messages == null || !messages.Any()) return messages;

                    var count = messages.Count;

                    var lastOffset = messages.Last().MessageOffset;

                    if (count + _nextOffset != lastOffset + 1)
                    {
                        var error = $"PullMessage offset payloadCount out-of-sync,topic={_topicName},leader={_consumer.Config.Broker},partition={_partition},payloadCount={count},FetchOffset={_nextOffset},lastOffset={lastOffset},retryCount={retryCount},maxRetry={maxRetry}";
                        GetNextOffset();
                    }

                    return messages;
                }
                catch (Exception)
                {
                    if (retryCount >= maxRetry)
                    {
                        throw;
                    }
                    GetNextOffset();
                }
                finally
                {
                    retryCount++;
                }
            }
            return null;
        }

        public void Shutdown()
        {
            _running = false;
            _handler.Set();
        }

        public void Dispose()
        {
            if (_running) Shutdown();

            _consumer?.Dispose();
            _consumer = null;

            _manager?.Dispose();
            _manager = null;
        }
    }
}
