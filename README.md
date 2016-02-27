# kafkanet-tests
A few tests to measure the performance/stability of the microsoft/kafkanet library

## Consumer

Fires up a High Level Consumer and listens for messages with timestamps.

    .\Consumer.exe [broker_ip] [group_name] [topic_name]

### Output

    ***** Timers - 2016-02-27T12:45:40.2972Z *****

        [Consumer] Received
       Active Sessions = 0
                 Count = 691900 Events
            Mean Value = 5530.93 Events/s
         1 Minute Rate = 4969.27 Events/s
         5 Minute Rate = 1902.37 Events/s
        15 Minute Rate = 719.98 Events/s
                 Count = 691900 Events
                  Last = 33.00 ms
                   Min = 0.00 ms
                   Max = 1047.00 ms
                  Mean = 503.52 ms
                StdDev = 292.89 ms
                Median = 510.00 ms
                  75% <= 767.00 ms
                  95% <= 964.00 ms
                  98% <= 1001.00 ms
                  99% <= 1017.00 ms
                99.9% <= 1030.00 ms


## Producer

Firest up a Simple Producer and sends messages with timestamps.

    .\Producer.exe [sookeeper_connect] [topic_name]

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
