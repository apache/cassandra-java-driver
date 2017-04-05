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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE;
import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD;
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;

@CreateCCM(PER_METHOD)
@CCMConfig(auth = false)
public class SSLEncryptionTest extends SSLTestBase {

    /**
     * <p>
     * Validates that an SSL connection can be established without client auth if the target
     * cassandra cluster is using SSL and does not require auth.
     * </p>
     *
     * @test_category connection:ssl
     * @expected_result Connection can be established to a cassandra node using SSL.
     */
    @Test(groups = "short", dataProvider = "sslImplementation", dataProviderClass = SSLTestBase.class)
    public void should_connect_with_ssl_without_client_auth_and_node_doesnt_require_auth(SslImplementation sslImplementation) throws Exception {
        connectWithSSLOptions(getSSLOptions(sslImplementation, false, true));
    }

    /**
     * <p>
     * Validates that an SSL connection can not be established if the client does not trust
     * the cassandra node's certificate.
     * </p>
     *
     * @test_category connection:ssl
     * @expected_result Connection can not be established to a cassandra node using SSL with an untrusted cert.
     */
    @Test(groups = "short", dataProvider = "sslImplementation", dataProviderClass = SSLTestBase.class, expectedExceptions = {NoHostAvailableException.class})
    public void should_not_connect_with_ssl_without_trusting_server_cert(SslImplementation sslImplementation) throws Exception {
        connectWithSSLOptions(getSSLOptions(sslImplementation, false, false));
    }

    /**
     * <p>
     * Validates that an SSL connection can not be established if the client is not specifying SSL, but
     * the cassandra node is using SSL.
     * </p>
     * <p/>
     * <p>
     * Note that future versions of cassandra may support both SSL-encrypted and non-SSL connections
     * simultaneously (CASSANDRA-8803)
     * </p>
     *
     * @test_category connection:ssl
     * @expected_result Connection can not be established to a cassandra node using SSL and the client not using SSL.
     */
    @Test(groups = "short", expectedExceptions = {NoHostAvailableException.class})
    public void should_not_connect_without_ssl_but_node_uses_ssl() throws Exception {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .build());
        cluster.connect();
    }

    /**
     * <p>
     * Validates that if connection is lost to a node that is using SSL authentication, that connection can be
     * re-established when the node becomes available again.
     * </p>
     *
     * @test_category connection:ssl
     * @expected_result Connection is re-established within a sufficient amount of time after a node comes back online.
     */
    @CCMConfig(dirtiesContext = true)
    @Test(groups = "long", dataProvider = "sslImplementation", dataProviderClass = SSLTestBase.class)
    public void should_reconnect_with_ssl_on_node_up(SslImplementation sslImplementation) throws Exception {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(this.getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withSSL(getSSLOptions(sslImplementation, true, true))
                .build());

        cluster.connect();

        ccm().stop(1);
        ccm().start(1);

        assertThat(cluster).host(1).comesUpWithin(TestUtils.TEST_BASE_NODE_WAIT, TimeUnit.SECONDS);
    }

    /**
     * <p>
     * Validates that SSL connectivity can be configured via the standard javax.net.ssl System properties.
     * </p>
     *
     * @test_category connection:ssl
     * @expected_result Connection can be established.
     */
    @Test(groups = "isolated")
    public void should_use_system_properties_with_default_ssl_options() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath());
        System.setProperty("javax.net.ssl.trustStorePassword", DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);
        try {
            connectWithSSL();
        } finally {
            try {
                System.clearProperty("javax.net.ssl.trustStore");
                System.clearProperty("javax.net.ssl.trustStorePassword");
            } catch (SecurityException e) {
                // ok
            }
        }
    }

}
