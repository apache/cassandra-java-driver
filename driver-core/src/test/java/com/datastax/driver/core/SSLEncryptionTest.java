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
     * Validates that an SSL connection can not be established if the
     * the cassandra node's certificate does not match the ip or hostname of the node (127.0.0.1). This will be checked by
     * either using the CN or the Subject Alternative Name of the certificate. The test certificate does not have
     * neither a matching CN or SAN set.
     * </p>
     *
     * @test_category connection:ssl
     * @expected_result Connection can not be established to a cassandra node after hostname verification.
     */
    @Test(groups="short", expectedExceptions={NoHostAvailableException.class})
    public void should_not_connect_using_server_hostname_verification() throws Exception {
        connectWithSSLOptions(getSSLOptions(Optional.<String>absent(), Optional.of(DEFAULT_CLIENT_TRUSTSTORE_PATH), true));
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
