using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Kafka.Client.Consumers;
using Moq;
using Ploeh.AutoFixture;
using Ploeh.AutoFixture.Xunit2;
using Xunit;
using Kafka.Client.Cfg;

namespace Kafka.Basic.Test
{
    public class KafkaConsumerInstance
    {
        private Fixture _fixture;
        private IKafkaConsumerInstance _consumerInstance;
        private ZookeeperConsumerConnector _zkConnector;

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
            _fixture = new Fixture();
        }


        private void CreateZookeeperConnector()
        {
            _zkConnector = _fixture.Create<ZookeeperConsumerConnector>();
        }

        private void CreateConsumerInstance()
        {
            //CreateZookeeperConnector();
            _consumerInstance = new Basic.KafkaConsumerInstance(_zkConnector);
        }


        [Theory, AutoData]
        public void Subscribe(string topicName)
        {
            var stream = _consumerInstance.Subscribe(topicName);

            Assert.NotNull(stream);
        }
    }
}
