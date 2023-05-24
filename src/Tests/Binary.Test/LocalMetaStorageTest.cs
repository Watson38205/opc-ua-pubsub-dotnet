// Copyright 2023 Siemens AG
// SPDX-License-Identifier: MIT

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using opc.ua.pubsub.dotnet.binary.Decode;
using opc.ua.pubsub.dotnet.binary.Messages;
using opc.ua.pubsub.dotnet.binary.Messages.Meta;
using opc.ua.pubsub.dotnet.client;

namespace opc.ua.pubsub.dotnet.binary.test;

[TestFixture]
internal class LocalMetaStorageTest
{
    private readonly string                       m_DiskMetaMessageCacheDirectory = Path.Combine( TestContext.CurrentContext.TestDirectory, ".meta" );
    private          Mock<ILogger<DecodeMessage>> m_Logger;
    private          DecodeMessage                m_DecodeMessage;

    [TestCaseSource( nameof( SaveLocalMetaStorageToDiskTestCases ) )]
    public void SaveLocalMetaStorageToDiskTest( bool legacyFieldFlagEncoding, bool cacheMetaMessageToDisk, int numberOfMetaMessages, int numberOfMetaFiles, int numberOfSaveMessageLogs, int numberOfDeletedMessageLogs )
    {
        // Arrange
        EncodingOptions encodingOptions = new EncodingOptions()
                                          {
                                                  LegacyFieldFlagEncoding = legacyFieldFlagEncoding,
                                                  DiskMetaMessageCacheDirectory = cacheMetaMessageToDisk ? m_DiskMetaMessageCacheDirectory : null,
                                          };

        // Create a new set of meta frames
        List<byte[]> encodedMetaFrames = new List<byte[]>();
        for ( int metaFrameIndex = 0; metaFrameIndex < numberOfMetaMessages; metaFrameIndex++ )
        {
            ProcessDataSet dataSet     = new ProcessDataSet( "test-publisher", $"test{metaFrameIndex:000}", 123, ProcessDataSet.DataSetType.TimeSeries );
            byte[]         encodedMeta = dataSet.GetEncodedMetaFrame( encodingOptions, 1, true );
            encodedMetaFrames.Add( encodedMeta );
        }
        m_DecodeMessage = new DecodeMessage( m_Logger.Object, encodingOptions );
        m_Logger.VerifyLog( LogLevel.Information, Times.Never(), "Storing meta message for" );

        // Act
        foreach ( byte[] encodedMetaFrame in encodedMetaFrames )
        {
            NetworkMessage metaNetworkMessage = m_DecodeMessage.ParseBinaryMessage( encodedMetaFrame );
            Assert.IsInstanceOf<MetaFrame>( metaNetworkMessage );
        }

        // Assert
        List<string> cachedMetaFiles = MetaMessageCacheFiles( m_DiskMetaMessageCacheDirectory );
        Assert.That( cachedMetaFiles.Count, Is.EqualTo( numberOfMetaFiles ) );

        m_Logger.VerifyLog( LogLevel.Information, Times.Exactly( numberOfMetaMessages ),       "Storing meta message for" );
        m_Logger.VerifyLog( LogLevel.Information, Times.Exactly( numberOfSaveMessageLogs ),    "Writing to disk meta frame" );
        m_Logger.VerifyLog( LogLevel.Information, Times.Exactly( numberOfDeletedMessageLogs ), "Deleted disk meta frame" );
    }

    private static IEnumerable<TestCaseData> SaveLocalMetaStorageToDiskTestCases()
    {
        yield return new TestCaseData( true, true, 1, 1 , 1, 0)
               .SetName( "{m}: Given 1 legacy meta frames and disk caching specified, when message(s) are decoded, then 1 files are stored." );
        yield return new TestCaseData( true, true, 2, 2, 2, 0 )
               .SetName( "{m}: Given 2 legacy meta frames and disk caching specified, when message(s) are decoded, then 2 files are stored." );
        yield return new TestCaseData( true, true, 3, 3 , 3, 0 )
               .SetName( "{m}: Given 3 legacy meta frames and disk caching specified, when message(s) are decoded, then 3 files are stored." );
        yield return new TestCaseData( true, true, 4, 4 , 4, 0 )
               .SetName( "{m}: Given 4 legacy meta frames and disk caching specified, when message(s) are decoded, then 4 files are stored." );
        yield return new TestCaseData( true, true, 5, 5 , 5, 0 )
               .SetName( "{m}: Given 5 legacy meta frames and disk caching specified, when message(s) are decoded, then 5 files are stored." );
        yield return new TestCaseData( true, true, 6, 6 , 6, 0 )
               .SetName( "{m}: Given 6 legacy meta frames and disk caching specified, when message(s) are decoded, then 6 files are stored." );
        yield return new TestCaseData( true, true, 7, 7 , 7, 0 )
               .SetName( "{m}: Given 7 legacy meta frames and disk caching specified, when message(s) are decoded, then 7 files are stored." );
        yield return new TestCaseData( true, true, 8, 8 , 8, 0 )
               .SetName( "{m}: Given 8 legacy meta frames and disk caching specified, when message(s) are decoded, then 8 files are stored." );
        yield return new TestCaseData( true, true, 9, 9 , 9, 0 )
               .SetName( "{m}: Given 9 legacy meta frames and disk caching specified, when message(s) are decoded, then 9 files are stored." );
        yield return new TestCaseData( true, true, 10, 10 , 10, 0 )
               .SetName( "{m}: Given 10 legacy meta frames and disk caching specified, when message(s) are decoded, then 10 files are stored." );
        yield return new TestCaseData( true, true, 11, 10, 11, 1 )
               .SetName( "{m}: Given 11 legacy meta frames and disk caching specified, when message(s) are decoded, then 10 files are stored." );
        yield return new TestCaseData( true, false, 11, 0 , 0, 0)
               .SetName( "{m}: Given 11 legacy meta frames and disk caching NOT specified, when message(s) are decoded, then 0 files are stored." );
        yield return new TestCaseData( false, true, 1, 1, 1, 0 )
               .SetName( "{m}: Given 1 meta frames and disk caching specified, when message(s) are decoded, then 1 files are stored." );
        yield return new TestCaseData( false, true, 2, 2, 2, 0 )
               .SetName( "{m}: Given 2 meta frames and disk caching specified, when message(s) are decoded, then 2 files are stored." );
        yield return new TestCaseData( false, true, 3, 3, 3, 0 )
               .SetName( "{m}: Given 3 meta frames and disk caching specified, when message(s) are decoded, then 3 files are stored." );
        yield return new TestCaseData( false, true, 4, 4, 4, 0 )
               .SetName( "{m}: Given 4 meta frames and disk caching specified, when message(s) are decoded, then 4 files are stored." );
        yield return new TestCaseData( false, true, 5, 5, 5, 0 )
               .SetName( "{m}: Given 5 meta frames and disk caching specified, when message(s) are decoded, then 5 files are stored." );
        yield return new TestCaseData( false, true, 6, 6, 6, 0 )
               .SetName( "{m}: Given 6 meta frames and disk caching specified, when message(s) are decoded, then 6 files are stored." );
        yield return new TestCaseData( false, true, 7, 7, 7, 0 )
               .SetName( "{m}: Given 7 meta frames and disk caching specified, when message(s) are decoded, then 7 files are stored." );
        yield return new TestCaseData( false, true, 8, 8, 8, 0 )
               .SetName( "{m}: Given 8 meta frames and disk caching specified, when message(s) are decoded, then 8 files are stored." );
        yield return new TestCaseData( false, true, 9, 9, 9, 0 )
               .SetName( "{m}: Given 9 meta frames and disk caching specified, when message(s) are decoded, then 9 files are stored." );
        yield return new TestCaseData( false, true, 10, 10, 10, 0 )
               .SetName( "{m}: Given 10 meta frames and disk caching specified, when message(s) are decoded, then 10 files are stored." );
        yield return new TestCaseData( false, true, 11, 10, 11, 1 )
               .SetName( "{m}: Given 11 meta frames and disk caching specified, when message(s) are decoded, then 10 files are stored." );
        yield return new TestCaseData( false, false, 11, 0, 0, 0 )
               .SetName( "{m}: Given 11 meta frames and disk caching NOT specified, when message(s) are decoded, then 0 files are stored." );
    }

    [TestCaseSource( nameof( LoadLocalMetaStorageFromDiskTestCases ) )]
    public void LoadLocalMetaStorageFromDiskTest( bool legacyFieldFlagEncoding)
    {
        // Arrange
        EncodingOptions encodingOptions = new EncodingOptions()
        {
            LegacyFieldFlagEncoding = legacyFieldFlagEncoding,
            DiskMetaMessageCacheDirectory = m_DiskMetaMessageCacheDirectory
        };

        // Save a meta frame to the cache directory
        ProcessDataSet dataSet            = new ProcessDataSet( "test-publisher", "Test001", 123, ProcessDataSet.DataSetType.TimeSeries );
        byte[]         encodedMeta        = dataSet.GetEncodedMetaFrame( encodingOptions, 1, true );
        var            saveDecodeMessage  = new DecodeMessage( Mock.Of<ILogger<DecodeMessage>>(), encodingOptions );
        NetworkMessage metaNetworkMessage = saveDecodeMessage.ParseBinaryMessage( encodedMeta );
        Assert.IsInstanceOf<MetaFrame>( metaNetworkMessage );
        List<string>   cachedMetaFiles   = MetaMessageCacheFiles( m_DiskMetaMessageCacheDirectory );
        Assert.That( cachedMetaFiles.Count, Is.EqualTo( 1 ) );
        
        // Act
        m_DecodeMessage = new DecodeMessage( m_Logger.Object, encodingOptions );

        // Assert
        m_Logger.VerifyLog( LogLevel.Information, Times.Once(), "Loading from disk meta frame" );
        m_Logger.VerifyLog( LogLevel.Information, Times.Once(), "Storing meta message for" );
    }

    private static IEnumerable<TestCaseData> LoadLocalMetaStorageFromDiskTestCases()
    {
        yield return new TestCaseData( true )
               .SetName( "{m}: Given 1 legacy meta frame that has been cached, when the DecodeMessage is constructed, then 1 files are stored." );
        yield return new TestCaseData( false )
               .SetName( "{m}: Given 1 meta frame that has been cached, when the DecodeMessage is constructed, then 1 files are stored." );
    }

    private static void DeleteMetaMessageCacheDirectory( string directory )
    {
        if ( string.IsNullOrEmpty( directory ) )
        {
            return;
        }

        if ( Directory.Exists( directory ) )
        {
            Directory.Delete( directory, true );
        }
    }

    private static List<string> MetaMessageCacheFiles( string directory )
    {
        if ( string.IsNullOrEmpty( directory ) )
        {
            return new List<string>();
        }

        if ( !Directory.Exists( directory ) )
        {
            return new List<string>();
        }

        return Directory.EnumerateFiles( directory )
                        .ToList();
    }

    [SetUp]
    public void SetUp()
    {
        m_Logger = new Mock<ILogger<DecodeMessage>>();
        m_Logger.Setup(p => p.IsEnabled( LogLevel.Information ) ).Returns( true );
        DeleteMetaMessageCacheDirectory( m_DiskMetaMessageCacheDirectory );
    }
}

static class MockHelper
{
    public static void VerifyLog<T>( this Mock<ILogger<T>> logger, LogLevel level, Times times, string regex = null ) =>
            logger.Verify( m => m.Log(
                                      level,
                                      It.IsAny<EventId>(),
                                      It.Is<It.IsAnyType>( ( x, y ) => regex == null || Regex.IsMatch( x.ToString(), regex ) ),
                                      It.IsAny<Exception>(),
                                      It.IsAny<Func<It.IsAnyType, Exception, string>>() ),
                           times );
}