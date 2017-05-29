/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import io.netty.handler.ssl.SslContextBuilder;
import org.testng.annotations.Test;

import javax.net.ssl.*;
import java.net.Socket;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static io.netty.handler.ssl.SslProvider.OPENSSL;

@CreateCCM(PER_METHOD)
@CCMConfig(auth = false)
public class Jdk8SSLEncryptionTest extends SSLTestBase {

    /**
     * Validates that {@link RemoteEndpointAwareSSLOptions} implementations properly pass remote endpoint information
     * to the underlying {@link SSLEngine} that is created.  This is done by creating a custom {@link TrustManagerFactory}
     * that inspects the peer information on the {@link SSLEngine} in
     * {@link X509ExtendedTrustManager#checkServerTrusted(X509Certificate[], String, SSLEngine)} and throws a
     * {@link CertificateException} if the peer host or port do not match.
     * <p>
     * This test is prefixed with 'Jdk8' so it only runs against JDK 8+ runtimes.  This is required because
     * X509ExtendedTrustManager was added in JDK 7.  Technically this would also run against JDK 7, but for simplicity
     * we only run it against 8+.
     *
     * @test_category connection:ssl
     * @jira_ticket JAVA-1364
     * @since 3.2.0
     */
    @Test(groups = "short", dataProvider = "sslImplementation", dataProviderClass = SSLTestBase.class)
    public void should_pass_peer_address_to_engine(SslImplementation sslImplementation) throws Exception {
        String expectedPeerHost = TestUtils.IP_PREFIX + "1";
        int expectedPeerPort = ccm().getBinaryPort();

        EngineInspectingTrustManagerFactory tmf = new EngineInspectingTrustManagerFactory(expectedPeerHost, expectedPeerPort);
        SSLOptions options = null;
        switch (sslImplementation) {
            case JDK:
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());
                SSLParameters parameters = sslContext.getDefaultSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                options = RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();
                break;
            case NETTY_OPENSSL:
                SslContextBuilder builder = SslContextBuilder
                        .forClient()
                        .sslProvider(OPENSSL)
                        .trustManager(tmf);

                options = new RemoteEndpointAwareNettySSLOptions(builder.build());
        }

        connectWithSSLOptions(options);
    }

    static class EngineInspectingTrustManagerFactory extends TrustManagerFactory {

        private static final Provider provider = new Provider("", 0.0, "") {

        };

        final EngineInspectingTrustManagerFactorySpi spi;

        EngineInspectingTrustManagerFactory(String expectedPeerHost, int expectedPeerPort) {
            this(new EngineInspectingTrustManagerFactorySpi(expectedPeerHost, expectedPeerPort));
        }

        private EngineInspectingTrustManagerFactory(EngineInspectingTrustManagerFactorySpi spi) {
            super(spi, provider, "EngineInspectingTrustManagerFactory");
            this.spi = spi;
        }
    }

    static class EngineInspectingTrustManagerFactorySpi extends TrustManagerFactorySpi {

        String expectedPeerHost;
        int expectedPeerPort;

        private final TrustManager tm = new X509ExtendedTrustManager() {

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType, SSLEngine sslEngine) throws CertificateException {
                // Capture peer address information and compare it to expectation.
                String peerHost = sslEngine.getPeerHost();
                int peerPort = sslEngine.getPeerPort();
                if (peerHost == null || !peerHost.equals(expectedPeerHost)) {
                    throw new CertificateException(String.format("Expected SSLEngine.getPeerHost() (%s) to equal (%s)", peerHost, expectedPeerHost));
                }
                if (peerPort != expectedPeerPort) {
                    throw new CertificateException(String.format("Expected SSLEngine.getPeerPort() (%d) to equal (%d)", peerPort, expectedPeerPort));
                }
            }

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType, Socket socket) throws CertificateException {
                // no op
            }

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
                // no op
            }

            @Override
            public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
                // Since we are doing server trust only, this is a no op.
                throw new UnsupportedOperationException("TrustManger is for establishing server trust only.");

            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType, Socket socket) throws CertificateException {
                // Since we are doing server trust only, this is a no op.
                throw new UnsupportedOperationException("TrustManger is for establishing server trust only.");
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType) throws CertificateException {
                // Since we are doing server trust only, this is a no op.
                throw new UnsupportedOperationException("TrustManger is for establishing server trust only.");
            }


            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };

        EngineInspectingTrustManagerFactorySpi(String expectedPeerHost, int expectedPeerPort) {
            this.expectedPeerHost = expectedPeerHost;
            this.expectedPeerPort = expectedPeerPort;
        }

        @Override
        protected void engineInit(KeyStore keyStore) throws KeyStoreException {
            // no op
        }

        @Override
        protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws InvalidAlgorithmParameterException {
            // no op
        }

        @Override
        protected TrustManager[] engineGetTrustManagers() {
            return new TrustManager[]{tm};
        }
    }
}
