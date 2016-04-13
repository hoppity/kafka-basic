using Kafka.Client.Messages;
using Kafka.Client.Producers;

namespace Kafka.Basic
{
    public static class MessageExtensions
    {
        public static ProducerData<string, Client.Messages.Message> AsProducerData(this Message message, string topic)
        {
            return new ProducerData<string, Client.Messages.Message>(
                topic,
                message.Key,
                new Client.Messages.Message(
                    message.Value.Encode(),
                    message.Key.Encode(),
                    (CompressionCodecs)message.Codec
                    )
                );
        }

        public static ConsumedMessage AsConsumedMessage(this Client.Messages.Message message)
        {
            return new ConsumedMessage
            {
                Partition = message.PartitionId ?? 0,
                Offset = message.Offset,
                Key = message.Key.Decode(),
                Value = message.Payload.Decode(),
                Codec = (Compression)message.CompressionCodec
            };
        }
    }
}