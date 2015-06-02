/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import org.testng.annotations.Test;

import com.datastax.driver.core.exceptions.NoHostAvailableException;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_KEYSTORE_PATH;
import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_PATH;

public class SSLEncryptionTest extends SSLTestBase {

    public SSLEncryptionTest() {
        super(false);
    }

    /**
     * <p>
     * Validates that an SSL connection can be established without client auth if the target
     * cassandra cluster is using SSL and does not require auth.
     * </p>
     *
     * @test_category connection:ssl
     * @expected_result Connection can be established to a cassandra node using SSL.
     */
    @Test(groups="short")
    public void should_connect_with_ssl_without_client_auth_and_node_doesnt_require_auth() throws Exception {
        connectWithSSLOptions(getSSLOptions(Optional.<String>absent(), Optional.of(DEFAULT_CLIENT_TRUSTSTORE_PATH)));
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
    @Test(groups="short", expectedExceptions={NoHostAvailableException.class})
    public void should_not_connect_with_ssl_without_trusting_server_cert() throws Exception {
        connectWithSSLOptions(getSSLOptions(Optional.<String>absent(), Optional.<String>absent()));
    }

    /**
     * <p>
     * Validates that an SSL connection can not be established if the client is not specifying SSL, but
     * the cassandra node is using SSL.
     * </p>
     *
     * <p>
     * Note that future versions of cassandra may support both SSL-encrypted and non-SSL connections
     * simultaneously (CASSANDRA-8803)
     * </p>
     *
     * @test_category connection:ssl
     * @expected_result Connection can not be established to a cassandra node using SSL and the client not using SSL.
     */
    @Test(groups="short", expectedExceptions={NoHostAvailableException.class})
    public void should_not_connect_without_ssl_but_node_uses_ssl() throws Exception {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.IP_PREFIX + '1')
                .build();

            cluster.connect();
        } finally {
            if (cluster != null)
                cluster.close();
        }
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
    @Test(groups="long")
    public void should_reconnect_with_ssl_on_node_up() throws Exception {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.IP_PREFIX + '1')
                .withSSL(getSSLOptions(Optional.of(DEFAULT_CLIENT_KEYSTORE_PATH), Optional.of(DEFAULT_CLIENT_TRUSTSTORE_PATH)))
                .build();

            cluster.connect();

            ccm.stop(1);
            ccm.start(1);

            assertThat(cluster).host(1).comesUpWithin(TestUtils.TEST_BASE_NODE_WAIT, TimeUnit.SECONDS);
        } finally {
            if (cluster != null)
                cluster.close();
        }
    }
}
