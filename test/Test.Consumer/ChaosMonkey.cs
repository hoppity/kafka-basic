using System;
using System.Linq;
using System.Threading;
using Metrics;

namespace Consumer
{
    class ChaosMonkey
    {
        static void WriteLog(string message, Exception exception = null)
        {
            Console.WriteLine(DateTime.UtcNow.ToString("O") + "-" + message + (exception != null ? ": " + exception : string.Empty));
        }

        public int Start(ChaosMonkeyOptions opts)
        {
            var histogram = Metric.Histogram("batch.size", Unit.Items);
            var timer = Metric.Timer("message.latency", Unit.Events);

            var random = new Random();
            var exceptionModulus = random.Next(76, 100);
            var waitModulus = random.Next(50, 75);

            using (var consumer = new Kafka.Basic.BatchedConsumer(opts.ZkConnect, opts.Group, opts.Topic, opts.Threads, opts.BatchTimeoutMs, opts.MaxBatchSize))
            {
                var call = 0;
                consumer
                    .Start(
                        m =>
                        {
                            var time = DateTime.UtcNow.Ticks;

                            call++;
                            var list = m.ToList();

                            WriteLog($"Received batch {call} of {list.Count} messages:");

                            list
                                .ForEach(message =>
                                {
                                    var value = long.Parse(message.Value);
                                    var diff = (time - value) / 10000;
                                    timer.Record(diff, TimeUnit.Milliseconds);
                                    WriteLog($"P:{message.Partition},O:{message.Offset},L:{diff}ms");
                                });

                            if (call % exceptionModulus == 0)
                            {
                                WriteLog($"Faking an exception to test shutdown behaviour... Expect to see the above {list.Count} messages come through again.");
                                throw new ChaosException();
                            }

                            if (call % waitModulus == 0)
                            {
                                WriteLog("Faking a long consume operation by sending the current thread to sleep for 5s.");
                                Thread.Sleep(5000);
                            }

                            histogram.Update(list.Count);
                        },
                        e =>
                        {
                            if (e is ChaosException)
                                WriteLog("Chaos achieved and handled.");
                            else
                                WriteLog("Got an error... ", e);
                        });
            }

            return 0;
        }

        private class ChaosException : Exception { }
    }
}
