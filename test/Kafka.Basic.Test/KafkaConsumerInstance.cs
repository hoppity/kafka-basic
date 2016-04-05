using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Moq;
using Ploeh.AutoFixture;
using Ploeh.AutoFixture.Xunit2;
using Ploeh.AutoFixture.AutoMoq;
using Xunit;

namespace Kafka.Basic.Test
{
    public class KafkaConsumerInstance
    {
        private IFixture _fixture;
        private IKafkaConsumerInstance _consumerInstance;
        private Mock<IZookeeperConnection> _zkConnectorMock;
        private Mock<IBalancedConsumer> _balancedConsumer; 

        public KafkaConsumerInstance()
        {
            Setup();
        }


        private void Setup()
        {
            CreateFixture();
            CreateConsumerInstance();
        }

        private void CreateFixture()
        {
            _fixture = new Fixture().Customize(new AutoMoqCustomization());
        }


        private void SetupMocks()
        {
            _zkConnectorMock = _fixture.Create<Mock<IZookeeperConnection>>();
            _balancedConsumer = _fixture.Create<Mock<IBalancedConsumer>>();

            _zkConnectorMock
                .Setup(z => z.CreateConsumerConnector(It.IsAny<ConsumerOptions>()))
                .Returns(_balancedConsumer.Object);
        }


        private void CreateConsumerInstance()
        {
            SetupMocks();
            _consumerInstance = new Basic.KafkaConsumerInstance(_zkConnectorMock.Object, new ConsumerOptions());
        }


        [Theory, AutoData]
        public void Subscribe(string topicName)
        {
            var stream = _consumerInstance.Subscribe(topicName);

            Assert.NotNull(stream);
        }


        [Theory, AutoData]
        public void Commit()
        {
            _consumerInstance.Commit();
            _balancedConsumer
                .Verify(mock => mock.CommitOffsets(),
                Times.AtLeastOnce());
        }


        [Theory, AutoData]
        public void ShutDown()
        {
            _consumerInstance.Shutdown();
            _balancedConsumer
                .Verify(mock => mock.CommitOffsets(), 
                Times.Once());

            _balancedConsumer
                .Verify(mock => mock.ReleaseAllPartitionOwnerships(),
                Times.Once());
        }


        [Theory, AutoData]
        public void Dispose()
        {
            _consumerInstance.Dispose();
            _balancedConsumer
                .Verify(mock => mock.Dispose(),
                Times.Once());
        }
    }
}
