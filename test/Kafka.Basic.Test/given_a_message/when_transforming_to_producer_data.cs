using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentAssertions;
using Kafka.Client.Messages;
using Kafka.Client.Producers;
using Xunit;
using KafkaMessage = Kafka.Client.Messages.Message;

namespace Kafka.Basic.Test.given_a_message
{
    public class when_transforming_to_producer_data
    {
        [Fact]
        public void then_key_and_value_should_be_set()
        {
            var message = new Message
            {
                Key = "key1",
                Value = "value1"
            };
            var expected = new ProducerData<string, KafkaMessage>(
                "topic",
                "key1",
                new KafkaMessage(Encoding.UTF8.GetBytes("value1"))
                );
            var actual = message.AsProducerData("topic");

            actual.ShouldBeEquivalentTo(expected);
        }

        [Theory, MemberData("CompressionCodecMappings")]
        public void then_codec_should_be_set(Compression codec, CompressionCodecs expected)
        {
            var message = new Message
            {
                Key = "key1",
                Value = "value1",
                Codec = codec
            };
            
            var actual = message.AsProducerData("topic");

            actual.Data.First().CompressionCodec.ShouldBeEquivalentTo(expected);
        }

        public static IEnumerable<object[]> CompressionCodecMappings => new[]
        {
            new object[] { Compression.None, CompressionCodecs.NoCompressionCodec },
            new object[] { Compression.Default, CompressionCodecs.GZIPCompressionCodec }, // Default is GZip
            new object[] { Compression.GZip, CompressionCodecs.GZIPCompressionCodec },
            new object[] { Compression.Snappy, CompressionCodecs.SnappyCompressionCodec }
        };
    }
}
