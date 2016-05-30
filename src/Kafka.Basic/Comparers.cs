using System.Collections.Generic;
using KafkaMessage = Kafka.Client.Messages.Message;

namespace Kafka.Basic
{
    public class KafkaMessageCompare : IEqualityComparer<KafkaMessage>
    {
        public bool Equals(KafkaMessage x, KafkaMessage y)
        {
            return (x.Offset == y.Offset && x.PartitionId == y.PartitionId);
        }

        public int GetHashCode(KafkaMessage obj)
        {
            return obj.Offset.GetHashCode();
        }
    }


    public class ConsumedMessageCompare : IEqualityComparer<ConsumedMessage>
    {
        public bool Equals(ConsumedMessage x, ConsumedMessage y)
        {
            return (x.Key == y.Key && x.Offset == y.Offset && x.Partition == y.Partition && x.Value == y.Value);
        }

        public int GetHashCode(ConsumedMessage obj)
        {
            return obj.Offset.GetHashCode();
        }
    }
}
