// Copyright 2020 Siemens AG
// SPDX-License-Identifier: MIT

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using opc.ua.pubsub.dotnet.binary.Header;
using opc.ua.pubsub.dotnet.binary.Messages.Meta;

namespace opc.ua.pubsub.dotnet.binary.Storage
{
    public class LocalMetaStorage
    {
        private readonly ILogger         m_Logger;
        private readonly EncodingOptions m_EncodingOptions;
        private const    int             MaximumMetaMessagePerPublisher = 10;
        
        /// <summary>
        ///     Used for storing the meta message only locally, not in Azure.
        ///     This is only used if the corresponding FeatureToggle is set.
        /// </summary>
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<ushort, ConcurrentDictionary<ConfigurationVersion, MetaFrame>>> m_DeviceMetaMessages = new();

        public LocalMetaStorage(ILogger logger, EncodingOptions encodingOptions )
        {
            m_Logger             = logger          ?? throw new ArgumentNullException( nameof(logger) );
            m_EncodingOptions    = encodingOptions ?? throw new ArgumentNullException( nameof( encodingOptions ) );
            LoadDiskMetaFrameCache();
        }

        public bool IsMetaMessageAlreadyKnown( string publisherID, ushort writerID, ConfigurationVersion cfg )
        {
            if ( string.IsNullOrWhiteSpace( publisherID ) )
            {
                return false;
            }
            if ( !m_DeviceMetaMessages.ContainsKey( publisherID ) )
            {
                return false;
            }
            ConcurrentDictionary<ushort, ConcurrentDictionary<ConfigurationVersion, MetaFrame>> writerIDs = m_DeviceMetaMessages[publisherID];
            if ( !writerIDs.ContainsKey( writerID ) )
            {
                return false;
            }
            ConcurrentDictionary<ConfigurationVersion, MetaFrame> metaMessages = writerIDs[writerID];
            return metaMessages.ContainsKey( cfg );
        }

        public MetaFrame RetrieveMetaMessageLocally( string publisherId, ushort writerID, ConfigurationVersion cfgVersion )
        {
            if ( string.IsNullOrEmpty( publisherId ) )
            {
                return null;
            }
            if ( !m_DeviceMetaMessages.TryGetValue( publisherId,
                                                    out ConcurrentDictionary<ushort, ConcurrentDictionary<ConfigurationVersion, MetaFrame>> deviceMetaMessages
                                                  ) )
            {
                return null;
            }
            if ( !deviceMetaMessages.TryGetValue( writerID, out ConcurrentDictionary<ConfigurationVersion, MetaFrame> metaDictionary ) )
            {
                return null;
            }
            if ( !metaDictionary.TryGetValue( cfgVersion, out MetaFrame metaMessage ) )
            {
                return null;
            }
            return metaMessage;
        }

        public void StoreMetaMessageLocally( MetaFrame metaFrame ) => StoreMetaMessageLocally( metaFrame, true );

        private void StoreMetaMessageLocally( MetaFrame metaFrame, bool cacheLocalDisk )
        {
            string               pubID         = metaFrame.NetworkMessageHeader.PublisherID.Value;
            ConfigurationVersion configVersion = metaFrame.ConfigurationVersion;
            ushort               writerID      = metaFrame.DataSetWriterID;
            if ( IsMetaMessageAlreadyKnown( pubID, writerID, configVersion ) )
            {
                return;
            }
            if ( m_Logger.IsEnabled(LogLevel.Information) )
            {
                m_Logger.LogInformation( $"Storing meta message for {pubID} with version {configVersion}." );
            }
            if ( !m_DeviceMetaMessages.TryGetValue( pubID, out ConcurrentDictionary<ushort, ConcurrentDictionary<ConfigurationVersion, MetaFrame>> publisherDictionary ) )
            {
                publisherDictionary = new ConcurrentDictionary<ushort, ConcurrentDictionary<ConfigurationVersion, MetaFrame>>();
                m_DeviceMetaMessages.TryAdd( pubID, publisherDictionary );
            }
            if ( !publisherDictionary.TryGetValue( writerID, out ConcurrentDictionary<ConfigurationVersion, MetaFrame> cfgDictionary ) )
            {
                cfgDictionary = new ConcurrentDictionary<ConfigurationVersion, MetaFrame>();
                publisherDictionary.TryAdd( writerID, cfgDictionary );
            }
            if ( cfgDictionary.Count >= MaximumMetaMessagePerPublisher )
            {
                cfgDictionary.TryRemove( cfgDictionary.Keys.OrderBy( a => a ).First(), out MetaFrame removedMetaFrame );
                RemoveMetaFrameFromDisk( removedMetaFrame );
            }

            // Add the Meta Message if it doesn't exist yet or if it's already present, replace it.
            cfgDictionary.AddOrUpdate( configVersion, metaFrame, ( existingVersion, existingFrame ) => metaFrame );

            // Only cache to local disk if required and cache directory specified
            if ( cacheLocalDisk  && !string.IsNullOrWhiteSpace(m_EncodingOptions.DiskMetaMessageCacheDirectory))
            {
                SaveMetaFrameToDisk( metaFrame, pubID, writerID, configVersion );
            }
        }

        /// <summary>
        /// Loads the local disk meta frames into the cache.
        /// </summary>
        private void LoadDiskMetaFrameCache()
        {
            // Check whether the cache directory is specified
            var metaDirectory = m_EncodingOptions.DiskMetaMessageCacheDirectory;
            if ( !Directory.Exists( metaDirectory ) )
            {
                return;
            }

            // Process all the meta frames cache in the directory
            foreach ( string fileName in Directory.EnumerateFiles( metaDirectory, "*.meta" ) )
            {
                m_Logger.LogInformation( "Loading from disk meta frame {FilePath}", fileName );
                using MemoryStream   memoryStream         = new MemoryStream( File.ReadAllBytes( fileName ) );
                NetworkMessageHeader networkMessageHeader = NetworkMessageHeader.Decode( memoryStream );
                MetaFrame            metaFrame            = MetaFrame.Decode( m_Logger, memoryStream, m_EncodingOptions);
                metaFrame.NetworkMessageHeader            = networkMessageHeader;
                StoreMetaMessageLocally( metaFrame, false );
            }
        }

        /// <summary>
        /// Saves the meta frame to the local disk as a caching mechanism.
        /// </summary>
        private void SaveMetaFrameToDisk( MetaFrame metaFrame, string publisherId, Int32 writerID, ConfigurationVersion configurationVersion )
        {
            var    metaDirectory = m_EncodingOptions.DiskMetaMessageCacheDirectory;
            string fileName      = $"{metaDirectory}\\{publisherId}-{writerID}-{configurationVersion.Major}-{configurationVersion.Minor}.meta";
            Directory.CreateDirectory( Path.GetDirectoryName( fileName ) );
            using MemoryStream memoryStream = new MemoryStream();
            metaFrame.Encode( m_Logger, memoryStream );
            m_Logger.LogInformation( "Writing to disk meta frame {FilePath}", fileName );
            File.WriteAllBytes( fileName, memoryStream.ToArray() );
        }

        /// <summary>
        /// Removes the meta frame from the local disk.
        /// </summary>
        /// <param name="metaFrame">The meta frame.</param>
        private void RemoveMetaFrameFromDisk( MetaFrame metaFrame )
        {
            // Check whether the cache directory is specified
            var metaDirectory = m_EncodingOptions.DiskMetaMessageCacheDirectory;
            if ( !Directory.Exists( metaDirectory ) )
            {
                return;
            }

            // Delete the meta frame file
            string               publisherId          = metaFrame.NetworkMessageHeader.PublisherID.Value;
            ConfigurationVersion configurationVersion = metaFrame.ConfigurationVersion;
            ushort               writerID             = metaFrame.DataSetWriterID;
            string               fileName             = $"{metaDirectory}\\{publisherId}-{writerID}-{configurationVersion.Major}-{configurationVersion.Minor}.meta";

            try
            {
                File.Delete( fileName );
                m_Logger.LogInformation( "Deleted disk meta frame {FilePath}", fileName );
            }
            catch ( Exception e )
            {
                m_Logger.LogError(e, "Failed to delete disk meta frame {FilePath}", fileName );
            }
        }
    }
}