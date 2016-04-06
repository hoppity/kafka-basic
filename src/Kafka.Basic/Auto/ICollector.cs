using System.Collections.Generic;

namespace Kafka.Basic.Auto
{
    public interface ICollector : IList<ConsumedMessage> { }
}