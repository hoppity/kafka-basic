using System.Configuration;

namespace Kafka.Basic
{
    public static class KafkaConfig
    {
        private static readonly object Lock = new object();

        static KafkaConfig()
        {
            ClientId = ConfigurationManager.AppSettings["kafka.client.id"] ?? "kafka.basic";
        }
        public static string ClientId { get; }
        public const short VersionId = 0;
        public static int CorrelationId { get; private set; }

        public static int NextCorrelationId()
        {
            lock (Lock)
            {
                return CorrelationId++;
            }
        }
    }
}