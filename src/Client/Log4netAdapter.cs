// Copyright 2020 Siemens AG
// SPDX-License-Identifier: MIT

using System;
using log4net;
using Microsoft.Extensions.Logging;

namespace opc.ua.pubsub.dotnet.client
{
    public class Log4netAdapter<T> : ILogger<T> where T : class
    {
        private readonly ILog m_Log;

        public Log4netAdapter( ILog log )
        {
            m_Log = log ?? throw new ArgumentNullException( nameof(log) );
        }

        public static ILogger<T> CreateLogger()
        {
            ILog log = log4net.LogManager.GetLogger(nameof(T));

            return new Log4netAdapter<T>( log );
        }

        public void Log<TState>( LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter )
        {
            string message = "Invalid Message";
            if ( formatter != null )
            {
                message = formatter( state, exception );
            }
            switch ( logLevel )
            {
                case LogLevel.Trace:
                case LogLevel.Debug:
                    m_Log.Debug( message, exception );
                    break;
                case LogLevel.Information:
                    m_Log.Info( message, exception );
                    break;
                case LogLevel.Warning:
                    m_Log.Warn( message, exception );
                    break;
                case LogLevel.Error:
                    m_Log.Error( message, exception );
                    break;
                case LogLevel.Critical:
                    m_Log.Fatal( message, exception );
                    break;
                case LogLevel.None:
                default:
                    break;
            }
        }

        public bool IsEnabled( LogLevel logLevel )
        {
            return logLevel switch
                   {
                           LogLevel.Trace       => m_Log.IsDebugEnabled,
                           LogLevel.Debug       => m_Log.IsDebugEnabled,
                           LogLevel.Information => m_Log.IsInfoEnabled,
                           LogLevel.Warning     => m_Log.IsWarnEnabled,
                           LogLevel.Error       => m_Log.IsErrorEnabled,
                           LogLevel.Critical    => m_Log.IsFatalEnabled,
                           LogLevel.None        => false,
                           _                    => throw new ArgumentOutOfRangeException( nameof(logLevel), logLevel, null )
                   };
        }

        public IDisposable BeginScope<TState>( TState state )
        {
            throw new NotImplementedException();
        }
    }
}
