// Copyright 2020 Siemens AG
// SPDX-License-Identifier: MIT

using System;
using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;
using opc.ua.pubsub.dotnet.binary.Header;

namespace opc.ua.pubsub.dotnet.binary.Messages
{
    public class NetworkMessage : ICodable<NetworkMessage>
    {
        public NetworkMessage() : this( new EncodingOptions() ) { }

        public NetworkMessage( EncodingOptions options )
        {
            Options = options;
        }

        public         NetworkMessageHeader NetworkMessageHeader                                                  { get; set; }
        public         byte[]               RawPayload                                                            { get; set; }
        public virtual void                 Encode( ILogger logger, Stream outputStream, bool withHeader = true ) { }
        public virtual void                 Decode( ILogger logger, Stream inputStream ) { }

        public         EncodingOptions      Options { get; protected set; }

        #region Overrides of Object

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine( "=================================================================================================" );
            sb.AppendLine( "OPC UA PubSub Message - Missing Meta Message" );
            sb.AppendLine( "-------------------------------------------------------------------------------------------------" );
            sb.AppendLine( "Network Message" );

            //sb.AppendLine($" |  {"ExtendedFlags 1",10} | {"ExtendedFlags 2",10} | {"PublisherID",40}");
            sb.AppendLine( "-------------------------------------------------------------------------------------------------" );
            sb.AppendLine( System.Globalization.CultureInfo.InvariantCulture, $"{"Protocol Version:",-20} {NetworkMessageHeader.ProtocolVersion}" );
            sb.AppendLine( System.Globalization.CultureInfo.InvariantCulture, $"{"Flags:",-20} {NetworkMessageHeader.UADPFlags}" );
            sb.Append( System.Globalization.CultureInfo.InvariantCulture, $"{"ExtendedFlags 1: ",-20}" );
            if ( NetworkMessageHeader.ExtendedFlags1 != null )
            {
                sb.Append( System.Globalization.CultureInfo.InvariantCulture, $"{NetworkMessageHeader.ExtendedFlags1}" );
            }
            sb.Append( Environment.NewLine );
            sb.Append( System.Globalization.CultureInfo.InvariantCulture, $"{"ExtendedFlags 2:",-20}" );
            if ( NetworkMessageHeader.ExtendedFlags2 != null )
            {
                sb.Append( System.Globalization.CultureInfo.InvariantCulture, $"{NetworkMessageHeader.ExtendedFlags2}" );
            }
            sb.Append( Environment.NewLine );
            sb.Append( System.Globalization.CultureInfo.InvariantCulture, $"{"PublisherID:",-20}" );
            if ( NetworkMessageHeader.PublisherID != null )
            {
                sb.Append( System.Globalization.CultureInfo.InvariantCulture, $"{NetworkMessageHeader.PublisherID.Value}" );
            }
            sb.Append( Environment.NewLine );
            sb.AppendLine( "=================================================================================================" );
            sb.AppendLine();
            return sb.ToString();
        }

        #endregion
    }
}