using System;

namespace Kafka.Basic.Auto
{
    public class TopicAttribute : Attribute
    {
        public string Name { get; private set; }

        public TopicAttribute(string name)
        {
            Name = name;
        }
    }
}
