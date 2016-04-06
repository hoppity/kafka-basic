using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Kafka.Basic.Abstracted;
using log4net;

namespace Kafka.Basic.Auto
{
    public class KafkaHost
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(BatchedConsumer));

        private readonly IList<Tuple<IConsumer, IAbstractedConsumer>> _consumers = new List<Tuple<IConsumer, IAbstractedConsumer>>();

        private readonly IConsumerTupleFactory _factory;

        public KafkaHost(string zkConnect)
        {
            _factory = new ConsumerTupleFactory(new KafkaClient(zkConnect));
        }

        public KafkaHost(IConsumerTupleFactory factory)
        {
            _factory = factory;
        }

        public void Start()
        {
            Start(AppDomain.CurrentDomain.GetAssemblies());
        }

        public void Start(params Assembly[] assemblies)
        {
            Start(assemblies.SelectMany(a => a.ExportedTypes).ToArray());
        }

        public void Start(params Type[] types)
        {
            var consumerType = typeof(IConsumer);
            StartInternal(types.Where(t => consumerType.IsAssignableFrom(t)));
        }

        private void StartInternal(IEnumerable<Type> types)
        {
            foreach (var type in types)
            {
                var constructor = type.GetConstructor(new Type[0]);
                if (constructor == null)
                {
                    Logger.WarnFormat("Can not create consumer with type {0}. Type does not have a default constructor.", type.Name);
                    continue;
                }

                var consumer = (IConsumer)constructor.Invoke(new object[0]);
                if (string.IsNullOrWhiteSpace(consumer.Topic))
                {
                    Logger.WarnFormat("Can not create consumer with type {0}. Topic must not be empty or whitespace.", type.Name);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(consumer.Group))
                {
                    Logger.WarnFormat("Can not create consumer with type {0}. Group must not be empty or whitespace.", type.Name);
                    continue;
                }

                var consumerTuple = _factory.TryBuildFor(consumer);
                if (consumerTuple == null)
                {
                    Logger.DebugFormat("Can not create consumer with type {0}. No appropriate method found for consuming messages.", type.Name);
                    continue;
                }
                _consumers.Add(consumerTuple);
            }
        }

        public void Shutdown()
        {
            var tasks = _consumers
                .Select(c => Task.Run(() => c.Item2.Shutdown()))
                .ToArray();

            Task.WaitAll(tasks);
        }
    }
}
