// Copyright 2020 Siemens AG
// SPDX-License-Identifier: MIT

using System;
using System.Collections;
using opc.ua.pubsub.dotnet.binary.DataPoints;
using opc.ua.pubsub.dotnet.binary.Decode;
using opc.ua.pubsub.dotnet.binary.Messages;
using opc.ua.pubsub.dotnet.binary.Messages.Delta;
using opc.ua.pubsub.dotnet.binary.Messages.Key;
using opc.ua.pubsub.dotnet.binary.Messages.Meta;
using NUnit.Framework;
using opc.ua.pubsub.dotnet.client;
using opc.ua.pubsub.dotnet.binary.Header;
using System.Collections.Generic;
using Microsoft.Extensions.Logging.Abstractions;

namespace opc.ua.pubsub.dotnet.binary.test
{
    public class RoundTripWithSingleDataPoint
    {
        private DecodeMessage m_DecodeMessage;

        private static IEnumerable SingleItemTestCases
        {
            get
            {
                yield return new TestCaseData( new DPSEvent
                                               {
                                                       Name      = "Sample DPS",
                                                       Value     = 2,
                                                       Orcat     = OrcatConstants.Process,
                                                       Quality   = QualityConstants.Good,
                                                       Timestamp = DateTime.Now.ToFileTimeUtc()
                                               }
                                             ).SetName( "{m}-DPSEvent" );
                yield return new TestCaseData( new SPSEvent
                                               {
                                                       Name      = "Sample SPS",
                                                       Value     = true,
                                                       Orcat     = OrcatConstants.Remote_Control,
                                                       Quality   = QualityConstants.Failure,
                                                       Timestamp = DateTime.Now.ToFileTimeUtc()
                                               }
                                             ).SetName( "{m}-SPSEvent" );
                yield return new TestCaseData( new IntegerEvent
                                               {
                                                       Name      = "Sample IntegerEvent",
                                                       Value     = 1234567,
                                                       Orcat     = OrcatConstants.General_Interrogation,
                                                       Quality   = QualityConstants.Invalid,
                                                       Timestamp = DateTime.Now.ToFileTimeUtc()
                                               }
                                             ).SetName( "{m}-IntegerEvent" );
                yield return new TestCaseData( new MeasuredValue
                                               {
                                                       Name      = "Sample MeasuredValue",
                                                       Value     = 3.14F,
                                                       Orcat     = OrcatConstants.Remote_Control,
                                                       Quality   = QualityConstants.Bad_Reference,
                                                       Timestamp = DateTime.Now.ToFileTimeUtc()
                                               }
                                             ).SetName( "{m}-MeasuredValue" );
                yield return new TestCaseData( new ComplexMeasuredValue
                                               {
                                                       Name      = "Sample ComplexMeasuredValue",
                                                       Value     = 6.67430F,
                                                       Angle     = 90.2F,
                                                       Orcat     = OrcatConstants.Remote_Control,
                                                       Quality   = QualityConstants.Bad_Reference,
                                                       Timestamp = DateTime.Now.ToFileTimeUtc()
                                               }
                                             ).SetName( "{m}-ComplexMeasuredValue" );
                yield return new TestCaseData( new MeasuredValuesArray50
                                               {
                                                       Name      = "Sample MeasuredValuesArray50",
                                                       Value     = new[] { 1F, 2F, 3F },
                                                       Orcat     = OrcatConstants.Remote_Control,
                                                       Quality   = QualityConstants.Bad_Reference,
                                                       Timestamp = DateTime.Now.ToFileTimeUtc()
                                               }
                                             ).SetName( "{m}-MeasuredValuesArray50" );
                yield return new TestCaseData( new CounterValue
                                               {
                                                       Name      = "Sample CounterValue",
                                                       Value     = 987654321,
                                                       Orcat     = OrcatConstants.Unknown,
                                                       Quality   = QualityConstants.Old_Data,
                                                       Timestamp = DateTime.Now.ToFileTimeUtc()
                                               }
                                             ).SetName( "{m}-CounterValue" );
                yield return new TestCaseData( new StringEvent
                                               {
                                                       Name      = "Sample StringEvent",
                                                       Value     = "Hello � World",
                                                       Orcat     = OrcatConstants.Maintenance,
                                                       Quality   = QualityConstants.Test,
                                                       Timestamp = DateTime.Now.ToFileTimeUtc()
                                               }
                                             ).SetName( "{m}-StringEvent" );
            }
        }

        [OneTimeSetUp]
        public void Setup()
        {
            NullLogger<DecodeMessage> nullLogger = new NullLogger<DecodeMessage>();
            m_DecodeMessage = new DecodeMessage( nullLogger );
        }

        [TestCaseSource( nameof(SingleItemTestCases) )]
        public void TestDeltaFrame( ProcessDataPointValue dataPoint )
        {
            ProcessDataSet dataSet = new ProcessDataSet( "test-publisher", "test001", 123, ProcessDataSet.DataSetType.TimeSeries );
            dataSet.AddDataPoint( dataPoint );
            byte[]         encodedMeta        = dataSet.GetEncodedMetaFrame( new EncodingOptions(), 1, true );
            byte[]         encodedKey         = dataSet.GetEncodedDeltaFrame( 2 );
            NetworkMessage metaNetworkMessage = m_DecodeMessage.ParseBinaryMessage( encodedMeta );
            Assert.That( metaNetworkMessage, Is.Not.Null );
            Assert.That( metaNetworkMessage, Is.InstanceOf( typeof(MetaFrame) ) );
            Assert.That( metaNetworkMessage.NetworkMessageHeader.UADPFlags.HasFlag( UADPFlags.PayloadHeaderEnabled ), Is.False );
            NetworkMessage deltaNetworkMessage = m_DecodeMessage.ParseBinaryMessage( encodedKey );
            Assert.That( deltaNetworkMessage, Is.Not.Null );
            Assert.That( deltaNetworkMessage, Is.InstanceOf( typeof(DeltaFrame) ) );
            Assert.That( deltaNetworkMessage.NetworkMessageHeader.UADPFlags.HasFlag( UADPFlags.PayloadHeaderEnabled ), Is.True );
            DeltaFrame decodedDeltaMessage = (DeltaFrame)deltaNetworkMessage;
            Assert.That( decodedDeltaMessage,             Is.Not.Null );
            Assert.That( decodedDeltaMessage.Items,       Is.Not.Empty );
            Assert.That( decodedDeltaMessage.Items.Count, Is.EqualTo( 1 ) );
            Assert.That( decodedDeltaMessage.NetworkMessageHeader.UADPFlags.HasFlag( UADPFlags.PayloadHeaderEnabled ), Is.True );
            ProcessDataPointValue decodedDataPoint = (ProcessDataPointValue)decodedDeltaMessage.Items[0];
            Common.AssertDataPointsAreEqual( dataPoint, decodedDataPoint );
        }

        [TestCaseSource( nameof(SingleItemTestCases) )]
        public void TestKeyFrame( ProcessDataPointValue dataPoint )
        {
            ProcessDataSet dataSet = new ProcessDataSet( "test-publisher", "test001", 123, ProcessDataSet.DataSetType.TimeSeries );
            dataSet.AddDataPoint( dataPoint );
            byte[]         encodedMeta        = dataSet.GetEncodedMetaFrame( new EncodingOptions(), 1, true );
            byte[]         encodedKey         = dataSet.GetEncodedKeyFrame( 2 );
            NetworkMessage metaNetworkMessage = m_DecodeMessage.ParseBinaryMessage( encodedMeta );
            Assert.That( metaNetworkMessage, Is.Not.Null );
            Assert.That( metaNetworkMessage, Is.InstanceOf( typeof(MetaFrame) ) );
            Assert.That( metaNetworkMessage.NetworkMessageHeader.UADPFlags.HasFlag( UADPFlags.PayloadHeaderEnabled ), Is.False );
            NetworkMessage keyNetworkMessage = m_DecodeMessage.ParseBinaryMessage( encodedKey );
            Assert.That( keyNetworkMessage, Is.Not.Null );
            Assert.That( keyNetworkMessage, Is.InstanceOf( typeof(KeyFrame) ) );
            Assert.That( keyNetworkMessage.NetworkMessageHeader.UADPFlags.HasFlag( UADPFlags.PayloadHeaderEnabled ), Is.True );
            KeyFrame decodedKeyMessage = (KeyFrame)keyNetworkMessage;
            ProcessDataPointValue decodedDataPoint  = (ProcessDataPointValue)decodedKeyMessage.Items[0];
            Common.AssertDataPointsAreEqual( dataPoint, decodedDataPoint );
            Assert.That( decodedKeyMessage,             Is.Not.Null );
            Assert.That( decodedKeyMessage.Items,       Is.Not.Empty );
            Assert.That( decodedKeyMessage.Items.Count, Is.EqualTo( 1 ) );
            Assert.That( decodedKeyMessage.NetworkMessageHeader.UADPFlags.HasFlag( UADPFlags.PayloadHeaderEnabled ), Is.True );
        }

        [TestCaseSource( nameof( SingleItemTestCases ) )]
        public void TestMetaFrame( ProcessDataPointValue dataPoint )
        {
            ProcessDataSet dataSet = new ProcessDataSet( "test-publisher", "test001", 123, ProcessDataSet.DataSetType.TimeSeries );
            dataSet.AddDataPoint( dataPoint );

            byte[] encodedMeta = dataSet.GetEncodedMetaFrame( new EncodingOptions(), 1, true );
            NetworkMessage decodedMeta = m_DecodeMessage.ParseBinaryMessage( encodedMeta );
            Assert.That( decodedMeta, Is.Not.Null );
            Assert.That( decodedMeta, Is.InstanceOf( typeof( MetaFrame ) ) );
            Assert.That( decodedMeta.NetworkMessageHeader.ExtendedFlags2.MessageType, Is.EqualTo( Header.MessageType.DiscoveryResponse ) );
            Assert.That( decodedMeta.NetworkMessageHeader.ExtendedFlags2.Chunk, Is.False );
            Assert.That( decodedMeta.NetworkMessageHeader.UADPFlags.HasFlag( UADPFlags.PayloadHeaderEnabled ), Is.False );

            List<byte[]> encodedMetaChunkedZero = dataSet.GetChunkedMetaFrame( 0, new EncodingOptions(), 1 );
            NetworkMessage decodedMetaChunkedZero = m_DecodeMessage.ParseBinaryMessage( encodedMetaChunkedZero[0] );
            Assert.That( decodedMetaChunkedZero, Is.Not.Null );
            Assert.That( decodedMetaChunkedZero, Is.InstanceOf( typeof( MetaFrame ) ) );
            Assert.That( decodedMetaChunkedZero.NetworkMessageHeader.ExtendedFlags2.MessageType, Is.EqualTo( Header.MessageType.DiscoveryResponse ) );
            Assert.That( decodedMetaChunkedZero.NetworkMessageHeader.ExtendedFlags2.Chunk, Is.False );
            Assert.That( decodedMetaChunkedZero.NetworkMessageHeader.UADPFlags.HasFlag( UADPFlags.PayloadHeaderEnabled ), Is.False );

            List<byte[]> encodedMetaChunked = dataSet.GetChunkedMetaFrame( 1024 * 14, new EncodingOptions(), 1 );
            NetworkMessage decodedMetaChunked = m_DecodeMessage.ParseBinaryMessage( encodedMetaChunked[0] );
            Assert.That( decodedMetaChunked, Is.Not.Null );
            Assert.That( decodedMetaChunked, Is.InstanceOf( typeof( MetaFrame ) ) );
            Assert.That( decodedMetaChunked.NetworkMessageHeader.ExtendedFlags2.MessageType, Is.EqualTo( Header.MessageType.DiscoveryResponse ) );
            Assert.That( decodedMetaChunked.NetworkMessageHeader.ExtendedFlags2.Chunk, Is.True );
            Assert.That( decodedMetaChunked.NetworkMessageHeader.UADPFlags.HasFlag( UADPFlags.PayloadHeaderEnabled ), Is.True );

        }
    }
}