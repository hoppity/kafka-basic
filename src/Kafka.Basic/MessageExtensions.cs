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
    }
}