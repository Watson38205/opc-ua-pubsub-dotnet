// Copyright 2020 Siemens AG
// SPDX-License-Identifier: MIT

using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

namespace opc.ua.pubsub.dotnet.client.common
{
    public static class CertifacteLogging
    {
        public static void LogCertifacte( X509Certificate2 certificate, ILogger logger )
        {
            logger.LogError( $"Subject: {certificate.Subject}" );
            logger.LogError( $"Issuer: {certificate.Issuer}" );
            logger.LogError( $"Version: {certificate.Version}" );
            logger.LogError( $"Valid Date: {certificate.NotBefore}" );
            logger.LogError( $"Expiry Date: {certificate.NotAfter}" );
            logger.LogError( $"Thumbprint: {certificate.Thumbprint}" );
            logger.LogError( $"Serial Number: {certificate.SerialNumber}" );
            logger.LogError( $"Friendly Name: {certificate.PublicKey.Oid.FriendlyName}" );
            logger.LogError( $"Public Key Format: {certificate.PublicKey.EncodedKeyValue.Format( true )}" );
            logger.LogError( $"Raw Data Length: {certificate.RawData.Length}" );
            logger.LogError( $"Certificate to string: {certificate.ToString( true )}" );

            // Does not work with .NET Core 2.2 :-(
            //logger.Error($"Certificate to XML String: {certificate.PublicKey.Key.ToXmlString(false)}");
        }

        public static void LogCertificateChain( X509Chain chain, ILogger logger )
        {
            logger.LogError( "Chain Information" );
            logger.LogError( $"Chain revocation flag: {chain.ChainPolicy.RevocationFlag}" );
            logger.LogError( $"Chain revocation mode: {chain.ChainPolicy.RevocationMode}" );
            logger.LogError( $"Chain verification flag: {chain.ChainPolicy.VerificationFlags}" );
            logger.LogError( $"Chain verification time: {chain.ChainPolicy.VerificationTime}" );
            logger.LogError( $"Chain status length: {chain.ChainStatus.Length}" );
            logger.LogError( $"Chain application policy count: {chain.ChainPolicy.ApplicationPolicy.Count}" );
            logger.LogError( $"Chain certificate policy count: {chain.ChainPolicy.CertificatePolicy.Count}" );
            logger.LogError( "Chain Element Information" );
            logger.LogError( $"Number of chain elements: {chain.ChainElements.Count}" );
            logger.LogError( $"Chain elements synchronized? {chain.ChainElements.IsSynchronized}" );
            foreach ( X509ChainElement element in chain.ChainElements )
            {
                logger.LogError( $"Element issuer name: {element.Certificate.Issuer}" );
                logger.LogError( $"Element certificate valid until: {element.Certificate.NotAfter}" );
                logger.LogError( $"Element certificate is valid: {element.Certificate.Verify()}" );
                logger.LogError( $"Element error status length: {element.ChainElementStatus.Length}" );
                logger.LogError( $"Element information: {element.Information}" );
                logger.LogError( $"Number of element extensions: {element.Certificate.Extensions.Count}{1}" );
                if ( chain.ChainStatus.Length > 1 )
                {
                    for ( int index = 0; index < element.ChainElementStatus.Length; index++ )
                    {
                        logger.LogError( element.ChainElementStatus[index]
                                                .Status.ToString()
                                       );
                        logger.LogError( element.ChainElementStatus[index]
                                                .StatusInformation
                                       );
                    }
                }
            }
        }
    }
}