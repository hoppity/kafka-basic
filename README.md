# kafka-basic
A basic .NET API over the top of [microsoft/kafkanet](https://github.com/microsoft/kafkanet) that lets you get up and going with Kafka quickly and easily.

## Kafka.Basic Library

This is a simplified API for using KafkaNET.

Check out Test.Consumer/HighLevelConsumer.cs, Test.Consumer/SimpleConsumer.cs and Producer/Program.cs for actual usage...

### High Level (Balanced) Consumer

    using (var client = new KafkaClient(zkConnect))
    {
        var consumerGroup = client.Consumer(groupName);
        using (var instance = consumerGroup.Join())
        {
            instance.Subscribe(topicName)
                .Data(message =>
                {
                    // Do something with message...
                })
                .Start();
            handler.WaitOne(); // Block the thread from disposing everything
        }
    }

### Simple Consumer

    using (var client = new KafkaClient(zkConnect))
    using (var consumer = client.SimpleConsumer())
    {
        consumer.Subscribe(topicName, partition, offset)
            .Data(message =>
            {
                // Do something with message...
            })
            .Start();
        handler.WaitOne(); // Block the thread from disposing everything
    }

### High Level Producer

    using (var client = new KafkaClient(zkConnect))
    using (var topic = client.Topic(topicName))
    {
        topic.Send(batch);
    }

## Consumer Tests

Fires up a consumer and listens for messages with timestamps.

    # high-level (load balanced) consumer
    .\Consumer.exe balanced -z [zookeeper_connection] -g [group_name] -t [topic_name]

	# simple consumer
    .\Consumer.exe simple -z [zookeeper_connection] -t [topic_name] -p [partition]

### Output

    ***** Timers - 2016-03-28T04:27:33.9723Z *****

        [Consumer] Received
       Active Sessions = 0
                 Count = 259430 Events
            Mean Value = 7388.04 Events/s
         1 Minute Rate = 5797.24 Events/s
         5 Minute Rate = 4925.15 Events/s
        15 Minute Rate = 4726.25 Events/s
                 Count = 259430 Events
                  Last = 2.00 ms
                   Min = 0.00 ms
                   Max = 25.00 ms
                  Mean = 1.26 ms
                StdDev = 1.07 ms
                Median = 1.00 ms
                  75% <= 1.00 ms
                  95% <= 2.00 ms
                  98% <= 3.00 ms
                  99% <= 4.00 ms
                99.9% <= 25.00 ms

## Producer Tests

Firest up a Simple Producer and sends messages with timestamps.

    .\Producer.exe [zookeeper_connect] [topic_name]

### Output

    ***** Timers - 2016-02-27T12:45:42.1681Z *****

        [Producer] Sent
       Active Sessions = 0
                 Count = 7119 Events
            Mean Value = 59.27 Events/s
         1 Minute Rate = 58.86 Events/s
         5 Minute Rate = 59.07 Events/s
        15 Minute Rate = 59.03 Events/s
                 Count = 7119 Events
                  Last = 0.00 ms
                   Min = 0.00 ms
                   Max = 16.00 ms
                  Mean = 0.26 ms
                StdDev = 0.86 ms
                Median = 0.00 ms
                  75% <= 0.00 ms
                  95% <= 1.00 ms
                  98% <= 2.00 ms
                  99% <= 2.00 ms
                99.9% <= 16.00 ms
