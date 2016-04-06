using System;
using System.Reflection;
using System.Security.Policy;
using Kafka.Basic.Abstracted;
using Kafka.Basic.Auto;
using Moq;
using Xunit;

namespace Kafka.Basic.Test.given_an_auto_host
{
    public class when_a_type_is_a_valid_balanced_consumer
    {
        [Fact]
        public void it_should_be_resolved()
        {
            var factoryMock = new Mock<IConsumerTupleFactory>();

            factoryMock
                .Setup(f => f.TryBuildFor(It.IsAny<IConsumer>()))
                .Returns(
                    new Tuple<IConsumer, IAbstractedConsumer>(
                        new Mock<IConsumer>().Object,
                        new Mock<IAbstractedConsumer>().Object
                        )
                );

            var host = new KafkaHost(factoryMock.Object);

            var type = typeof(GoodBalancedConsumer);
            host.Start(type);

            var method = type.GetMethod("Receive");
            factoryMock.Verify(f => f.TryBuildFor(It.IsAny<GoodBalancedConsumer>()));
        }

        class GoodBalancedConsumer : IConsumer
        {
            public string Group { get; } = "fake.group";
            public string Topic { get; } = "fake.topic";

            public void Receive(ConsumedMessage message)
            {
                throw new NotImplementedException();
            }
        }
    }

    public class when_a_type_is_a_valid_batched_consumer
    {
        [Fact]
        public void it_should_be_resolved()
        {
            var factoryMock = new Mock<IConsumerTupleFactory>();

            factoryMock
                .Setup(f => f.TryBuildFor(It.IsAny<IConsumer>()))
                .Returns(
                    new Tuple<IConsumer, IAbstractedConsumer>(
                        new Mock<IConsumer>().Object,
                        new Mock<IAbstractedConsumer>().Object
                        )
                );

            var host = new KafkaHost(factoryMock.Object);

            var type = typeof(GoodBatchedConsumer);
            host.Start(type);

            var method = type.GetMethod("Receive");
            factoryMock.Verify(f => f.TryBuildFor(It.IsAny<GoodBatchedConsumer>()));
        }

        class GoodBatchedConsumer : IConsumer
        {
            public string Group { get; } = "fake.group";
            public string Topic { get; } = "fake.topic";

            public void Receive(IBatch messages)
            {
                throw new NotImplementedException();
            }
        }
    }

    public class when_a_type_is_not_a_consumer
    {
        [Fact]
        public void it_should_not_be_resolved()
        {
            var factoryMock = new Mock<IConsumerTupleFactory>();

            var host = new KafkaHost(factoryMock.Object);

            host.Start(typeof(NotAConsumer));

            factoryMock.VerifyNoConsumerResolved();
        }

        class NotAConsumer { }
    }

    public class when_a_consumer_does_not_have_a_default_constructor
    {
        [Fact]
        public void it_should_not_be_resolved()
        {
            var factoryMock = new Mock<IConsumerTupleFactory>();

            var host = new KafkaHost(factoryMock.Object);

            host.Start(typeof(BadConsumer));

            factoryMock.VerifyNoConsumerResolved();
        }

        class BadConsumer : IConsumer
        {
            public string Group { get; } = "fake.group";
            public string Topic { get; } = "fake.topic";

            public BadConsumer(string parameter) { }

            public void Receive(IBatch messages)
            {
                throw new NotImplementedException();
            }
        }
    }

    public class when_a_consumer_has_method_with_empty_topic
    {
        [Fact]
        public void it_should_not_be_resolved()
        {
            var factoryMock = new Mock<IConsumerTupleFactory>();

            var host = new KafkaHost(factoryMock.Object);

            host.Start(typeof(BadConsumer));

            factoryMock.VerifyNoConsumerResolved();
        }

        class BadConsumer : IConsumer
        {
            public string Group { get; } = "fake.group";
            public string Topic { get; } = " ";

            public void Receive(ConsumedMessage message)
            {
                throw new NotImplementedException();
            }
        }
    }

    public class when_a_consumer_has_method_with_unknown_type
    {
        [Fact]
        public void it_should_try_resolve()
        {
            var factoryMock = new Mock<IConsumerTupleFactory>();

            var host = new KafkaHost(factoryMock.Object);

            host.Start(typeof(BadConsumer));

            factoryMock.Verify(f => f.TryBuildFor(It.IsAny<BadConsumer>()));
        }

        class BadConsumer : IConsumer
        {
            public string Group { get; } = "fake.group";
            public string Topic { get; } = "fake.topic";

            public void Receive(DateTime date)
            {
                throw new NotImplementedException();
            }
        }
    }

    internal static class FactoryExtensions
    {
        public static void VerifyNoConsumerResolved(this Mock<IConsumerTupleFactory> factoryMock)
        {
            factoryMock.Verify(
                f => f.TryBuildFor(It.IsAny<IConsumer>()),
                Times.Never());
        }
    }
}
