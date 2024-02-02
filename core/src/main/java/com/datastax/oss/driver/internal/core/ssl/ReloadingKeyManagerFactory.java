/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.ssl;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadingKeyManagerFactory extends KeyManagerFactory implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ReloadingKeyManagerFactory.class);
  private static final String KEYSTORE_TYPE = "JKS";
  private Path keystorePath;
  private String keystorePassword;
  private ScheduledExecutorService executor;
  private final Spi spi;

  // We're using a single thread executor so this shouldn't need to be volatile, since all updates
  // to lastDigest should come from the same thread
  private volatile byte[] lastDigest;

  /**
   * Create a new {@link ReloadingKeyManagerFactory} with the given keystore file and password,
   * reloading from the file's content at the given interval. This function will do an initial
   * reload before returning, to confirm that the file exists and is readable.
   *
   * @param keystorePath the keystore file to reload
   * @param keystorePassword the keystore password
   * @param reloadInterval the duration between reload attempts. Set to {@link Optional#empty()} to
   *     disable scheduled reloading.
   * @return
   */
  static ReloadingKeyManagerFactory create(
      Path keystorePath, String keystorePassword, Optional<Duration> reloadInterval)
      throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException,
          CertificateException, IOException {
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

    KeyStore ks;
    try (InputStream ksf = Files.newInputStream(keystorePath)) {
      ks = KeyStore.getInstance(KEYSTORE_TYPE);
      ks.load(ksf, keystorePassword.toCharArray());
    }
    kmf.init(ks, keystorePassword.toCharArray());

    ReloadingKeyManagerFactory reloadingKeyManagerFactory = new ReloadingKeyManagerFactory(kmf);
    reloadingKeyManagerFactory.start(keystorePath, keystorePassword, reloadInterval);
    return reloadingKeyManagerFactory;
  }

  @VisibleForTesting
  protected ReloadingKeyManagerFactory(KeyManagerFactory initial) {
    this(
        new Spi((X509ExtendedKeyManager) initial.getKeyManagers()[0]),
        initial.getProvider(),
        initial.getAlgorithm());
  }

  private ReloadingKeyManagerFactory(Spi spi, Provider provider, String algorithm) {
    super(spi, provider, algorithm);
    this.spi = spi;
  }

  private void start(
      Path keystorePath, String keystorePassword, Optional<Duration> reloadInterval) {
    this.keystorePath = keystorePath;
    this.keystorePassword = keystorePassword;

    // Ensure that reload is called once synchronously, to make sure the file exists etc.
    reload();

    if (!reloadInterval.isPresent() || reloadInterval.get().isZero()) {
      final String msg =
          "KeyStore reloading is disabled. If your Cassandra cluster requires client certificates, "
              + "client application restarts are infrequent, and client certificates have short lifetimes, then your client "
              + "may fail to re-establish connections to Cassandra hosts. To enable KeyStore reloading, see "
              + "`advanced.ssl-engine-factory.keystore-reload-interval` in reference.conf.";
      logger.info(msg);
    } else {
      logger.info("KeyStore reloading is enabled with interval {}", reloadInterval.get());

      this.executor =
          Executors.newScheduledThreadPool(
              1,
              runnable -> {
                Thread t = Executors.defaultThreadFactory().newThread(runnable);
                t.setName(String.format("%s-%%d", this.getClass().getSimpleName()));
                t.setDaemon(true);
                return t;
              });
      this.executor.scheduleWithFixedDelay(
          this::reload,
          reloadInterval.get().toMillis(),
          reloadInterval.get().toMillis(),
          TimeUnit.MILLISECONDS);
    }
  }

  @VisibleForTesting
  void reload() {
    try {
      reload0();
    } catch (Exception e) {
      String msg =
          "Failed to reload KeyStore. If this continues to happen, your client may use stale identity"
              + " certificates and fail to re-establish connections to Cassandra hosts.";
      logger.warn(msg, e);
    }
  }

  private synchronized void reload0()
      throws NoSuchAlgorithmException, IOException, KeyStoreException, CertificateException,
          UnrecoverableKeyException {
    logger.debug("Checking KeyStore file {} for updates", keystorePath);

    final byte[] keyStoreBytes = Files.readAllBytes(keystorePath);
    final byte[] newDigest = digest(keyStoreBytes);
    if (lastDigest != null && Arrays.equals(lastDigest, digest(keyStoreBytes))) {
      logger.debug("KeyStore file content has not changed; skipping update");
      return;
    }

    final KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (InputStream inputStream = new ByteArrayInputStream(keyStoreBytes)) {
      keyStore.load(inputStream, keystorePassword.toCharArray());
    }
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, keystorePassword.toCharArray());
    logger.info("Detected updates to KeyStore file {}", keystorePath);

    this.spi.keyManager.set((X509ExtendedKeyManager) kmf.getKeyManagers()[0]);
    this.lastDigest = newDigest;
  }

  @Override
  public void close() throws Exception {
    if (executor != null) {
      executor.shutdown();
    }
  }

  private static byte[] digest(byte[] payload) throws NoSuchAlgorithmException {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    return digest.digest(payload);
  }

  private static class Spi extends KeyManagerFactorySpi {
    DelegatingKeyManager keyManager;

    Spi(X509ExtendedKeyManager initial) {
      this.keyManager = new DelegatingKeyManager(initial);
    }

    @Override
    protected void engineInit(KeyStore ks, char[] password) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void engineInit(ManagerFactoryParameters spec) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected KeyManager[] engineGetKeyManagers() {
      return new KeyManager[] {keyManager};
    }
  }

  private static class DelegatingKeyManager extends X509ExtendedKeyManager {
    AtomicReference<X509ExtendedKeyManager> delegate;

    DelegatingKeyManager(X509ExtendedKeyManager initial) {
      delegate = new AtomicReference<>(initial);
    }

    void set(X509ExtendedKeyManager keyManager) {
      delegate.set(keyManager);
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
      return delegate.get().chooseEngineClientAlias(keyType, issuers, engine);
    }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
      return delegate.get().chooseEngineServerAlias(keyType, issuers, engine);
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
      return delegate.get().getClientAliases(keyType, issuers);
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
      return delegate.get().chooseClientAlias(keyType, issuers, socket);
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
      return delegate.get().getServerAliases(keyType, issuers);
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
      return delegate.get().chooseServerAlias(keyType, issuers, socket);
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
      return delegate.get().getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
      return delegate.get().getPrivateKey(alias);
    }
  }
}
