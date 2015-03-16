package com.datastax.driver.core;

import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.exceptions.NoHostAvailableException;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD;
import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_KEYSTORE_PATH;
import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD;
import static com.datastax.driver.core.CCMBridge.DEFAULT_CLIENT_TRUSTSTORE_PATH;
public abstract class SSLTestBase {

    private final boolean requireClientAuth;

    protected CCMBridge ccm;

    public SSLTestBase(boolean requireClientAuth) {
        this.requireClientAuth = requireClientAuth;
    }

    @BeforeClass(groups={"short", "long"})
    public void beforeClass() {
        ccm = CCMBridge.create("test");
        ccm.enableSSL(requireClientAuth);
        ccm.bootstrapNode(1);
    }

    @AfterClass(groups={"short", "long"})
    public void afterClass() {
        ccm.remove();
    }

    /**
     * <p>
     * Attempts to connect to a cassandra cluster with the given SSLOptions and then closes the
     * created {@link Cluster} instance.
     * </p>
     *
     * @param sslOptions SSLOptions to use.
     * @throws Exception A {@link com.datastax.driver.core.exceptions.NoHostAvailableException} will be
     *  raised here if connection cannot be established.
     */
    protected void connectWithSSLOptions(SSLOptions sslOptions) throws Exception {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.IP_PREFIX + '1')
                .withSSL(sslOptions)
                .build();

            cluster.connect();
        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    /**
     * @param keyStorePath Path to keystore, if absent is not used.
     * @param trustStorePath Path to truststore, if absent is not used.
     * @return {@link com.datastax.driver.core.SSLOptions} with the given keystore and truststore path's for
     * server certificate validation and client certificate authentication.
     */
    public SSLOptions getSSLOptions(Optional<String> keyStorePath, Optional<String> trustStorePath) throws Exception {

        TrustManagerFactory tmf = null;
        if(trustStorePath.isPresent()) {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(this.getClass().getResourceAsStream(trustStorePath.get()), DEFAULT_CLIENT_TRUSTSTORE_PASSWORD.toCharArray());

            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
        }

        KeyManagerFactory kmf = null;
        if(keyStorePath.isPresent()) {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(this.getClass().getResourceAsStream(keyStorePath.get()), DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray());

            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray());
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf != null ? kmf.getKeyManagers() : null, tmf != null ? tmf.getTrustManagers() : null, new SecureRandom());

        return new SSLOptions(sslContext, SSLOptions.DEFAULT_SSL_CIPHER_SUITES);
    }
}
