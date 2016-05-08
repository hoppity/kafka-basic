using log4net;
using log4net.Repository.Hierarchy;
using Serilog;
using Serilog.Events;

namespace Kafka.Basic
{
    public static class KafkaLogging
    {
        private static bool _configured;
        public static void Configure(ILogger logger = null, LogEventLevel minimumLevel = LogEventLevel.Warning)
        {
            if (_configured) return;
            _configured = true;

            var serilogAppender = new SerilogAppender(logger, minimumLevel);
            serilogAppender.ActivateOptions();

            var loggerRepository = (Hierarchy)LogManager.GetRepository();
            loggerRepository.Root.AddAppender(serilogAppender);
            loggerRepository.Configured = true;
        }
    }
}
