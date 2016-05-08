using System;
using System.Linq.Expressions;
using log4net.Appender;
using log4net.Core;
using log4net.Util;
using Serilog.Events;
using ILogger = Serilog.ILogger;

namespace Kafka.Basic
{
    internal class SerilogAppender : AppenderSkeleton
    {
        private static readonly Func<SystemStringFormat, string> FormatGetter;
        private static readonly Func<SystemStringFormat, object[]> ArgumentsGetter;
        private readonly ILogger _logger;
        private readonly LogEventLevel _minimumLevel;

        static SerilogAppender()
        {
            FormatGetter = GetFieldAccessor<SystemStringFormat, string>("m_format");
            ArgumentsGetter = GetFieldAccessor<SystemStringFormat, object[]>("m_args");
        }

        public SerilogAppender(ILogger logger = null, LogEventLevel minimumLevel = LogEventLevel.Warning)
        {
            _logger = logger;
            _minimumLevel = minimumLevel;
        }

        protected override void Append(LoggingEvent loggingEvent)
        {
            var source = loggingEvent.LoggerName;
            var serilogLevel = ConvertLevel(loggingEvent.Level);
            if (serilogLevel < _minimumLevel) return;

            string template;
            object[] parameters = null;

            var systemStringFormat = loggingEvent.MessageObject as SystemStringFormat;
            if (systemStringFormat != null)
            {
                template = FormatGetter(systemStringFormat);
                parameters = ArgumentsGetter(systemStringFormat);
            }
            else
            {
                template = loggingEvent.MessageObject?.ToString();
            }

            var logger = (_logger ?? Serilog.Log.Logger).ForContext(Serilog.Core.Constants.SourceContextPropertyName, source);
            logger.Write(serilogLevel, loggingEvent.ExceptionObject, template, parameters);
        }

        private static LogEventLevel ConvertLevel(Level level)
        {
            if (level == Level.Verbose)
            {
                return LogEventLevel.Verbose;
            }
            if (level == Level.Debug)
            {
                return LogEventLevel.Debug;
            }
            if (level == Level.Info)
            {
                return LogEventLevel.Information;
            }
            if (level == Level.Warn)
            {
                return LogEventLevel.Warning;
            }
            if (level == Level.Error)
            {
                return LogEventLevel.Error;
            }
            if (level == Level.Fatal)
            {
                return LogEventLevel.Fatal;
            }
            Serilog.Debugging.SelfLog.WriteLine("Unexpected log4net logging minimumLevel ({0}) logging as Information", level.DisplayName);
            return LogEventLevel.Information;
        }

        //taken from http://rogeralsing.com/2008/02/26/linq-expressions-access-private-fields/
        public static Func<T, TField> GetFieldAccessor<T, TField>(string fieldName)
        {
            var param = Expression.Parameter(typeof(T), "arg");
            var member = Expression.Field(param, fieldName);
            var lambda = Expression.Lambda(typeof(Func<T, TField>), member, param);
            var compiled = (Func<T, TField>)lambda.Compile();
            return compiled;
        }
    }
}
