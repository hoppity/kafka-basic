using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Kafka.Basic.Auto
{
    public class KafkaHost
    {
        private readonly IKafkaClient _client;
        private readonly IList<IConsumer> _consumers = new List<IConsumer>();

        public KafkaHost(string zkConnect)
        {
            _client = new KafkaClient(zkConnect);
        }

        public void Start()
        {
            var consumerType = typeof(IConsumer);

            var types = AppDomain.CurrentDomain
                .GetAssemblies()
                .SelectMany(a => a.ExportedTypes)
                .Where(t => consumerType.IsAssignableFrom(t));

            foreach (var type in types)
            {
                var methods = type.GetMethods();
                foreach (var method in methods)
                {
                    if (method.GetParameters().Length > 1)
                    {
                        // only 1 parameter supported...
                        continue;
                    }

                    var firstParameter = method.GetParameters()
                        .FirstOrDefault();

                    var attributes = firstParameter?.GetCustomAttributes(typeof(TopicAttribute), false);
                    if (attributes == null || attributes.Length == 0)
                    {
                        // no parameters or first parameter does not have TopicAttribute...
                        // can't be a consumer.
                        continue;
                    }

                    if (attributes.Length > 1)
                    {
                        // only 1 topic supported...
                    }

                    var topicAttribute = (TopicAttribute)attributes[0];
                    if (string.IsNullOrWhiteSpace(topicAttribute.Name))
                    {
                        // topic name empty...
                        continue;
                    }

                    if (firstParameter.ParameterType == typeof(ConsumedMessage))
                    {
                        CreateBalancedConsumer(type, method, topicAttribute.Name);
                        continue;
                    }

                    if (firstParameter.ParameterType == typeof(IBatch))
                    {
                        // create a batched consumer
                        continue;
                    }
                }
            }
        }

        private void CreateBalancedConsumer(Type type, MethodInfo method, string topic)
        {
            var constructor = type.GetConstructor(new Type[0]);
            if (constructor == null)
            {
                // needs default constructor.
                return;
            }

            var consumer = (IConsumer)constructor.Invoke(new object[0]);
            var kafkaConsumer = _client.Consumer(new ConsumerOptions
            {
                AutoCommit = true,
                AutoOffsetReset = Offset.Earliest,
                GroupName = consumer.Group
            });

            var kafkaInstance = kafkaConsumer.Join();
            var kafkaStream = kafkaInstance
                .Subscribe(topic)
                .Data(m => method.Invoke(consumer, new object[] { m }));

             
        }
    }
}
