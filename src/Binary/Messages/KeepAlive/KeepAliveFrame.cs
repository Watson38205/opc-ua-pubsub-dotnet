// Copyright 2020 Siemens AG
// SPDX-License-Identifier: MIT

using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;

namespace opc.ua.pubsub.dotnet.binary.Messages.KeepAlive
{
    public class KeepAliveFrame : DataFrame
    {
        public KeepAliveFrame() { }
        public KeepAliveFrame( DataFrame dataFrame ) : base( dataFrame ) { }

        public static KeepAliveFrame Decode( Stream inputStream, DataFrame dataFrame )
        {
            if ( inputStream == null || !inputStream.CanRead )
            {
                return null;
            }
            KeepAliveFrame instance = new KeepAliveFrame( dataFrame );
            return instance;
        }

        public override void Encode( ILogger logger, Stream outputStream, bool withHeader = true )
        {
            if ( outputStream == null || !outputStream.CanWrite )
            {
                return;
            }
            base.Encode( logger, outputStream, withHeader );
        }

        public override void EncodeChunk( ILogger logger, Stream outputStream )
        {
            if ( outputStream == null || !outputStream.CanWrite )
            {
                return;
            }
            base.EncodeChunk( logger, outputStream );
        }

        #region Overrides of DataFrame

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine( "" );
            sb.AppendLine( "=============================================================================================================================================="
                         );
            sb.AppendLine( "Keep Alive Message" );
            sb.AppendLine( "=============================================================================================================================================="
                         );
            sb.AppendLine();
            return sb.ToString();
        }

        #endregion
    }
}