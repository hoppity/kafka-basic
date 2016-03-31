using System.Text;
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
                    Encoding.UTF8.GetBytes(message.Value),
                    Encoding.UTF8.GetBytes(message.Key),
                    (CompressionCodecs)message.Codec
                    )
                );
        }
    }
}