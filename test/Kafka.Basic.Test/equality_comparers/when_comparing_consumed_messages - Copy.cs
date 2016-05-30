using System.Collections.Generic;

using Xunit;
using FluentAssertions;

namespace Kafka.Basic.Test.equality_comparers
{
    public class when_comparing_consumed_messages
    {
        [Theory, MemberData("UnequalMessages")]
        public void should_fail_when_messages_are_different(ConsumedMessage message1, ConsumedMessage message2)
        {
            var comparer = new ConsumedMessageCompare();
            comparer.Equals(message1, message2).Should().BeFalse();
        }


        [Theory, MemberData("EqualMessages")]
        public void should_pass_when_messages_are_different(ConsumedMessage message1, ConsumedMessage message2)
        {
            var comparer = new ConsumedMessageCompare();
            comparer.Equals(message1, message2).Should().BeTrue();
        }


        public static IEnumerable<ConsumedMessage[]> UnequalMessages()
        {
            return new List<ConsumedMessage[]>
            {
                new [] {
                     new ConsumedMessage { Codec = Compression.Default, Key = "test", Offset = 1, Partition = 0, Value = "test" },
                     new ConsumedMessage { Codec = Compression.Default, Key = "test-fail", Offset = 1, Partition = 0, Value = "test" }
                },
                new [] {
                     new ConsumedMessage { Codec = Compression.Default, Key = "test", Offset = 1, Partition = 0, Value = "test" },
                     new ConsumedMessage { Codec = Compression.Default, Key = "test", Offset = 2, Partition = 0, Value = "test" }
                },
                new [] {
                     new ConsumedMessage { Codec = Compression.Default, Key = "test", Offset = 1, Partition = 0, Value = "test" },
                     new ConsumedMessage { Codec = Compression.Default, Key = "test", Offset = 1, Partition = 1, Value = "test" }
                },
                new [] {
                     new ConsumedMessage { Codec = Compression.Default, Key = "test", Offset = 1, Partition = 0, Value = "test" },
                     new ConsumedMessage { Codec = Compression.Default, Key = "test", Offset = 1, Partition = 0, Value = "test-fail" }
                }
            };
        }

        public static IEnumerable<ConsumedMessage[]> EqualMessages()
        {
            return new List<ConsumedMessage[]>
            {
                new [] {
                     new ConsumedMessage { Codec = Compression.Default, Key = "test", Offset = 1, Partition = 0, Value = "test" },
                     new ConsumedMessage { Codec = Compression.Default, Key = "test", Offset = 1, Partition = 0, Value = "test" }
                }
            };
        }

    }
}
