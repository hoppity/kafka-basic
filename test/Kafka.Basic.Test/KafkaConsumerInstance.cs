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
        private Mock<IConsumerConnector> _consumerConnector; 

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
            _consumerConnector = _fixture.Create<Mock<IConsumerConnector>>();

            _zkConnectorMock
                .Setup(z => z.CreateConsumerConnector(It.IsAny<ConsumerOptions>()))
                .Returns(_consumerConnector.Object);
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
            _consumerConnector
                .Verify(mock => mock.CommitOffsets(),
                Times.AtLeastOnce());
        }


        [Theory, AutoData]
        public void ShutDown()
        {
            _consumerInstance.Shutdown();

            _consumerConnector
                .Verify(mock => mock.ReleaseAllPartitionOwnerships(),
                Times.Once());
        }


        [Theory, AutoData]
        public void Dispose()
        {
            _consumerInstance.Dispose();
            _consumerConnector
                .Verify(mock => mock.Dispose(),
                Times.Once());
        }
    }
}
