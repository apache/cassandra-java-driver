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
import org.testng.annotations.DataProvider;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import java.security.SecureRandom;

import static com.datastax.driver.core.SSLTestBase.SslImplementation.JDK;
import static com.datastax.driver.core.SSLTestBase.SslImplementation.NETTY_OPENSSL;
import static io.netty.handler.ssl.SslProvider.OPENSSL;
import static org.assertj.core.api.Assertions.fail;

@CCMConfig(ssl = true, createCluster = false)
public abstract class SSLTestBase extends CCMTestsSupport {

    @DataProvider(name = "sslImplementation")
    public static Object[][] sslImplementation() {
        // Bypass Netty SSL if on JDK 1.6 since it only works on 1.7+.
        String javaVersion = System.getProperty("java.version");
        if (javaVersion.startsWith("1.6")) {
            return new Object[][]{{JDK}};
        } else {
            return new Object[][]{{JDK}, {NETTY_OPENSSL}};
        }
    }

    /**
     * <p>
     * Attempts to connect to a cassandra cluster with the given SSLOptions and then closes the
     * created {@link Cluster} instance.
     * </p>
     *
     * @param sslOptions SSLOptions to use.
     * @throws Exception A {@link com.datastax.driver.core.exceptions.NoHostAvailableException} will be
     *                   raised here if connection cannot be established.
     */
    protected void connectWithSSLOptions(SSLOptions sslOptions) throws Exception {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withSSL(sslOptions)
                .build());
        cluster.connect();
    }

    /**
     * <p>
     * Attempts to connect to a cassandra cluster with using {@link Cluster.Builder#withSSL} with no
     * provided {@link SSLOptions} and then closes the created {@link Cluster} instance.
     * </p>
     *
     * @throws Exception A {@link com.datastax.driver.core.exceptions.NoHostAvailableException} will be
     *                   raised here if connection cannot be established.
     */
    protected void connectWithSSL() throws Exception {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withSSL()
                .build());
        cluster.connect();
    }

    enum SslImplementation {JDK, NETTY_OPENSSL}

    /**
     * @param sslImplementation the SSL implementation to use
     * @param clientAuth        whether the client should authenticate
     * @param trustingServer    whether the client should trust the server's certificate
     * @return {@link com.datastax.driver.core.SSLOptions} with the given configuration for
     * server certificate validation and client certificate authentication.
     */
    public SSLOptions getSSLOptions(SslImplementation sslImplementation, boolean clientAuth, boolean trustingServer) throws Exception {

        TrustManagerFactory tmf = null;
        if (trustingServer) {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(
                    this.getClass().getResourceAsStream(CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_PATH),
                    CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD.toCharArray());

            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
        }


        switch (sslImplementation) {
            case JDK:
                KeyManagerFactory kmf = null;
                if (clientAuth) {
                    KeyStore ks = KeyStore.getInstance("JKS");
                    ks.load(
                            this.getClass().getResourceAsStream(CCMBridge.DEFAULT_CLIENT_KEYSTORE_PATH),
                            CCMBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray());

                    kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(ks, CCMBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray());
                }

                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(kmf != null ? kmf.getKeyManagers() : null, tmf != null ? tmf.getTrustManagers() : null, new SecureRandom());

                return RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();

            case NETTY_OPENSSL:
                SslContextBuilder builder = SslContextBuilder
                        .forClient()
                        .sslProvider(OPENSSL)
                        .trustManager(tmf);

                if (clientAuth) {
                    builder.keyManager(CCMBridge.DEFAULT_CLIENT_CERT_CHAIN_FILE, CCMBridge.DEFAULT_CLIENT_PRIVATE_KEY_FILE);
                }

                return new RemoteEndpointAwareNettySSLOptions(builder.build());
            default:
                fail("Unsupported SSL implementation: " + sslImplementation);
                return null;
        }
    }
}
