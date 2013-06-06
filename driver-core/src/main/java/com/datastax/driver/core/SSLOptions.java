package com.datastax.driver.core;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;

/**
 * Options to provide to enable SSL connections.
 */
public class SSLOptions {

    private static final String SSL_PROTOCOL = "TLS";

    /**
     * The default SSL cipher suites.
     */
    public static final String[] DEFAULT_SSL_CIPHER_SUITES = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" };

    final SSLContext context;
    final String[] cipherSuites;

    /**
     * Creates default SSL options.
     * <p>
     * The resulting options will use the default JSSE options, and you can use the default
     * <a href="http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization">JSSE System properties</a>
     * to customize it's behavior. This may in particular involve
     * <a href="http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#CreateKeystore">creating a simple keyStore and trustStore</a>.
     * <p>
     * The cipher suites used by this default instance are the one defined by
     * {@code DEFAULT_SSL_CIPHER_SUITES} and match the default cipher suites
     * supported by Cassandra server side.
     */
    public SSLOptions() {
        this(makeDefaultContext(), DEFAULT_SSL_CIPHER_SUITES);
    }

    /**
     * Creates SSL options that uses the provided SSL context and cipher suites.
     *
     * @param context the {@code SSLContext} to use.
     * @param cipherSuites the cipher suites to use.
     */
    public SSLOptions(SSLContext context, String[] cipherSuites) {
        this.context = context;
        this.cipherSuites = cipherSuites;
    }

    private static SSLContext makeDefaultContext() throws IllegalStateException {
        try {
            SSLContext ctx = SSLContext.getInstance(SSL_PROTOCOL);
            ctx.init(null, null, null); // use defaults
            return ctx;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("This JVM doesn't support TLS, this shouldn't happen");
        } catch (KeyManagementException e) {
            throw new IllegalStateException("Cannot initialize SSL Context", e);
        }
    }
}
