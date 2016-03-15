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
        private IZookeeperConsumerConnector _zkConnector;
        private Mock<IZookeeperConsumerConnector> _zkConnectorMock;

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


        private void CreateZookeeperConnector()
        {
            _zkConnectorMock = _fixture.Create<Mock<IZookeeperConsumerConnector>>();
        }


        private void CreateConsumerInstance()
        {
            CreateZookeeperConnector();
            _consumerInstance = new Basic.KafkaConsumerInstance(_zkConnectorMock.Object);
        }


        [Theory, AutoData]
        public void Subscribe(string topicName)
        {
            var stream = _consumerInstance.Subscribe(topicName);

            Assert.NotNull(stream);
        }


        [Theory, AutoData]
        public async void Commit()
        {
            await _consumerInstance.Commit();
            _zkConnectorMock
                .Verify(mock => mock.CommitOffsets(),
                Times.AtLeastOnce());
        }


        [Theory, AutoData]
        public async void ShutDown()
        {
            await _consumerInstance.Shutdown();
            _zkConnectorMock
                .Verify(mock => mock.CommitOffsets(), 
                Times.Once());

            _zkConnectorMock
                .Verify(mock => mock.ReleaseAllPartitionOwnerships(),
                Times.Once());
        }


        [Theory, AutoData]
        public void Dispose()
        {
            _consumerInstance.Dispose();
            _zkConnectorMock
                .Verify(mock => mock.Dispose(),
                Times.Once());
        }
    }
}
