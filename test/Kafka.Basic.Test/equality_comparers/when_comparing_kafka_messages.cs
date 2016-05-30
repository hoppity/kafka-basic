using System.Collections.Generic;

using Moq;
using Xunit;
using FluentAssertions;

using KafkaMessage = Kafka.Client.Messages.Message;

namespace Kafka.Basic.Test.equality_comparers
{
    public class when_comparing_kafka_messages
    {
        [Theory, MemberData("UnequalMessages")]
        public void should_fail_when_messages_are_different(KafkaMessage message1, KafkaMessage message2)
        {
            var comparer = new KafkaMessageCompare();
            comparer.Equals(message1, message2).Should().BeFalse();
        }


        [Theory, MemberData("EqualMessages")]
        public void should_pass_when_messages_are_different(KafkaMessage message1, KafkaMessage message2)
        {
            var comparer = new KafkaMessageCompare();
            comparer.Equals(message1, message2).Should().BeTrue();
        }


        public static IEnumerable<KafkaMessage[]> UnequalMessages()
        {
            return new List<KafkaMessage[]>
            {
                new [] {
                    CreateMessage(1, 100),
                    CreateMessage(3, 100)
                },
                new [] {
                    CreateMessage(1, 100),
                    CreateMessage(1, 200)
                },
                new [] {
                    CreateMessage(1, 100),
                    CreateMessage(3, 200)
                }
            };
        }

        public static IEnumerable<KafkaMessage[]> EqualMessages()
        {
            return new List<KafkaMessage[]>
            {
                new [] {
                    CreateMessage(1, 100),
                    CreateMessage(1, 100)
                }
            };
        }



        private static KafkaMessage CreateMessage(int partition, long offset)
        {
            var message = new KafkaMessage(new byte[] { });
            message.PartitionId = partition;
            message.Offset = offset;

            return message;
        }

    }
}
