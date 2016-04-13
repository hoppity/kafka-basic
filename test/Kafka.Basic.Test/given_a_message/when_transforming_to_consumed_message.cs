using FluentAssertions;
using Kafka.Client.Messages;
using Xunit;
using KafkaMessage = Kafka.Client.Messages.Message;

namespace Kafka.Basic.Test.given_a_message
{
    public class when_transforming_to_consumed_message
    {
        [Fact]
        public void then_properties_should_be_set()
        {
            var message = new KafkaMessage(
                "value1".Encode(),
                "key1".Encode(),
                CompressionCodecs.GZIPCompressionCodec
                )
            {
                PartitionId = 1,
                Offset = 1234
            };

            var actual = message.AsConsumedMessage();
            var expected = new ConsumedMessage
            {
                Codec = Compression.GZip,
                Key = "key1",
                Value = "value1",
                Partition = 1,
                Offset = 1234
            };

            actual.ShouldBeEquivalentTo(expected);
        }
    }
}
