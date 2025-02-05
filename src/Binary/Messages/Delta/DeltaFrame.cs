﻿// Copyright 2020 Siemens AG
// SPDX-License-Identifier: MIT

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;
using opc.ua.pubsub.dotnet.binary.DataPoints;
using opc.ua.pubsub.dotnet.binary.Messages.Meta;
using File = opc.ua.pubsub.dotnet.binary.DataPoints.File;

namespace opc.ua.pubsub.dotnet.binary.Messages.Delta
{
    public class DeltaFrame : DataFrame
    {
        public DeltaFrame() { }

        public DeltaFrame( DataFrame dataFrame ) : base( dataFrame )
        {
            FieldIndexList = new List<ushort>();
        }

        public ushort       FieldCount     { get; set; }
        public List<ushort> FieldIndexList { get; set; }

        public static DeltaFrame Decode( ILogger logger, Stream inputStream, DataFrame dataFrame, MetaFrame meta )
        {
            if ( inputStream == null || !inputStream.CanRead || meta == null )
            {
                return null;
            }
            DeltaFrame instance = new DeltaFrame( dataFrame );
            instance.MetaFrame = meta;
            instance.Timestamp = dataFrame.Timestamp;
            ushort? readuInt16 = BaseType.ReadUInt16( inputStream );
            if ( readuInt16 != null )
            {
                instance.FieldCount = readuInt16.Value;
            }
            instance.Items          = new List<DataPointValue>( instance.FieldCount );
            instance.FieldIndexList = new List<ushort>( instance.FieldCount );
            for ( int i = 0; i < instance.FieldCount; i++ )
            {
                // Index
                ushort? indexFromStream = BaseType.ReadUInt16( inputStream );
                if ( !indexFromStream.HasValue )
                {
                    throw new Exception( "Decode DeltaFrame: Could not parse field index." );
                }
                instance.FieldIndexList.Add( indexFromStream.Value );
                DataPointValue item = ParseDataPoint( logger, inputStream, meta, indexFromStream.Value );
                if ( item != null )
                {
                    instance.Items.Add( item );
                }
            }
            return instance;
        }

        public override void Encode( ILogger logger, Stream outputStream, bool withHeader = true )
        {
            if ( outputStream == null || !outputStream.CanWrite )
            {
                return;
            }
            base.Encode( logger, outputStream, withHeader );
            EncodeDataPoints( logger, outputStream );
        }

        public override void EncodeChunk( ILogger logger, Stream outputStream )
        {
            if ( outputStream == null || !outputStream.CanWrite )
            {
                return;
            }
            base.EncodeChunk( logger, outputStream );
            EncodeDataPoints( logger, outputStream );
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine( "" );
            sb.AppendLine( "=============================================================================================================================================="
                         );
            sb.AppendLine( "Delta Message" );
            sb.AppendLine( "----------------------------------------------------------------------------------------------------------------------------------------------"
                         );
            sb.AppendLine( System.Globalization.CultureInfo.InvariantCulture, $"{"Index",10} | {"Name",50} | {"Orcat",10} | {"Quality",10} | {"Custom",10} | {"MDSP q",10} | {"Timestamp",21} | {"Value",20}" );
            sb.AppendLine( "----------------------------------------------------------------------------------------------------------------------------------------------"
                         );
            for ( int i = 0; i < Items.Count; i++ )
            {
                DataPointValue dpv   = Items[i];
                string         name  = "";
                string         value = "";
                switch ( dpv )
                {
                    case ProcessDataPointValue pdv:
                        value = pdv.ToString();
                        break;

                    case File file:
                        value = file.ToString();
                        break;
                }
                if ( MetaFrame?.FieldMetaDataList != null && i < MetaFrame.FieldMetaDataList.Count )
                {
                    name = MetaFrame.FieldMetaDataList[dpv.Index]
                                    .Name.ToString();
                }
                sb.AppendLine( System.Globalization.CultureInfo.InvariantCulture, $"{FieldIndexList[i],10} | {name,-50} | {value}" );
            }
            sb.AppendLine( "=============================================================================================================================================="
                         );
            sb.AppendLine();
            return sb.ToString();
        }

        private void EncodeDataPoints( ILogger logger, Stream outputStream )
        {
            BaseType.WriteToStream( outputStream, BitConverter.GetBytes( FieldCount ) );
            if ( FieldIndexList.Count != Items.Count )
            {
                throw new Exception( $"Encode DeltaFrame: Mismatch between number of Items ({Items.Count}) and Field Indexes ({FieldIndexList.Count})." );
            }
            for ( int i = 0; i < Items.Count; i++ )
            {
                // Index
                BaseType.WriteToStream( outputStream, BitConverter.GetBytes( FieldIndexList[i] ) );

                // Value
                WriteSingleDataPoint( logger, outputStream, Items[i] );
            }
        }
    }
}