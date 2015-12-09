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

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.security.NoSuchAlgorithmException;

/**
 * {@link SSLOptions} implementation based on built-in JDK classes.
 */
public class JdkSSLOptions implements SSLOptions {

    /**
     * Creates a builder to create a new instance.
     *
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    private final SSLContext context;
    private final String[] cipherSuites;

    /**
     * Creates a new instance.
     *
     * @param context      the SSL context.
     * @param cipherSuites the cipher suites to use.
     */
    protected JdkSSLOptions(SSLContext context, String[] cipherSuites) {
        this.context = (context == null) ? makeDefaultContext() : context;
        this.cipherSuites = cipherSuites;
    }

    @Override
    public SslHandler newSSLHandler(SocketChannel channel) {
        SSLEngine engine = newSSLEngine(channel);
        return new SslHandler(engine);
    }

    /**
     * Creates an SSL engine each time a connection is established.
     * <p/>
     * <p/>
     * You might want to override this if you need to fine-tune the engine's configuration
     * (for example enabling hostname verification).
     *
     * @param channel the Netty channel for that connection.
     * @return the engine.
     */
    protected SSLEngine newSSLEngine(SocketChannel channel) {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(true);
        if (cipherSuites != null)
            engine.setEnabledCipherSuites(cipherSuites);
        return engine;
    }

    private static SSLContext makeDefaultContext() throws IllegalStateException {
        try {
            return SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Cannot initialize SSL Context", e);
        }
    }

    /**
     * Helper class to build JDK-based SSL options.
     */
    public static class Builder {
        private SSLContext context;
        private String[] cipherSuites;

        /**
         * Set the SSL context to use.
         * <p/>
         * If this method isn't called, a context with the default options will be used,
         * and you can use the default
         * <a href="http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization">JSSE System properties</a>
         * to customize its behavior. This may in particular involve
         * <a href="http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#CreateKeystore">creating a simple keyStore and trustStore</a>.
         *
         * @param context the SSL context.
         * @return this builder.
         */
        public Builder withSSLContext(SSLContext context) {
            this.context = context;
            return this;
        }

        /**
         * Set the cipher suites to use.
         * <p/>
         * If this method isn't called, the default is to present all the eligible client ciphers to the server.
         *
         * @param cipherSuites the cipher suites to use.
         * @return this builder.
         */
        public Builder withCipherSuites(String[] cipherSuites) {
            this.cipherSuites = cipherSuites;
            return this;
        }

        /**
         * Builds a new instance based on the parameters provided to this builder.
         *
         * @return the new instance.
         */
        public JdkSSLOptions build() {
            return new JdkSSLOptions(context, cipherSuites);
        }
    }
}
