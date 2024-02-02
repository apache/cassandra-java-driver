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

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadingKeyManagerFactoryTest {
  private static final Logger logger =
      LoggerFactory.getLogger(ReloadingKeyManagerFactoryTest.class);

  static final Path CERT_BASE =
      Paths.get(
          ReloadingKeyManagerFactoryTest.class
              .getResource(
                  String.format("/%s/certs/", ReloadingKeyManagerFactoryTest.class.getSimpleName()))
              .getPath());
  static final Path SERVER_KEYSTORE_PATH = CERT_BASE.resolve("server.keystore");
  static final Path SERVER_TRUSTSTORE_PATH = CERT_BASE.resolve("server.truststore");

  static final Path ORIGINAL_CLIENT_KEYSTORE_PATH = CERT_BASE.resolve("client-original.keystore");
  static final Path ALTERNATE_CLIENT_KEYSTORE_PATH = CERT_BASE.resolve("client-alternate.keystore");
  static final BigInteger ORIGINAL_CLIENT_KEYSTORE_CERT_SERIAL =
      convertSerial("7372a966"); // 1936894310
  static final BigInteger ALTERNATE_CLIENT_KEYSTORE_CERT_SERIAL =
      convertSerial("e50bf31"); // 240172849

  // File at this path will change content
  static final Path TMP_CLIENT_KEYSTORE_PATH;

  static {
    try {
      TMP_CLIENT_KEYSTORE_PATH =
          Files.createTempFile(ReloadingKeyManagerFactoryTest.class.getSimpleName(), null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static final Path CLIENT_TRUSTSTORE_PATH = CERT_BASE.resolve("client.truststore");
  static final String CERTSTORE_PASSWORD = "changeit";

  private static TrustManagerFactory buildTrustManagerFactory() {
    TrustManagerFactory tmf;
    try (InputStream tsf = Files.newInputStream(CLIENT_TRUSTSTORE_PATH)) {
      KeyStore ts = KeyStore.getInstance("JKS");
      char[] password = CERTSTORE_PASSWORD.toCharArray();
      ts.load(tsf, password);
      tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ts);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return tmf;
  }

  private static SSLContext buildServerSslContext() {
    try {
      SSLContext context = SSLContext.getInstance("SSL");

      TrustManagerFactory tmf;
      try (InputStream tsf = Files.newInputStream(SERVER_TRUSTSTORE_PATH)) {
        KeyStore ts = KeyStore.getInstance("JKS");
        char[] password = CERTSTORE_PASSWORD.toCharArray();
        ts.load(tsf, password);
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
      }

      KeyManagerFactory kmf;
      try (InputStream ksf = Files.newInputStream(SERVER_KEYSTORE_PATH)) {
        KeyStore ks = KeyStore.getInstance("JKS");
        char[] password = CERTSTORE_PASSWORD.toCharArray();
        ks.load(ksf, password);
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, password);
      }

      context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
      return context;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void client_certificates_should_reload() throws Exception {
    Files.copy(
        ORIGINAL_CLIENT_KEYSTORE_PATH, TMP_CLIENT_KEYSTORE_PATH, REPLACE_EXISTING, COPY_ATTRIBUTES);

    final BlockingQueue<Optional<X509Certificate[]>> peerCertificates =
        new LinkedBlockingQueue<>(1);

    // Create a listening socket. Make sure there's no backlog so each accept is in order.
    SSLContext serverSslContext = buildServerSslContext();
    final SSLServerSocket server =
        (SSLServerSocket) serverSslContext.getServerSocketFactory().createServerSocket();
    server.bind(new InetSocketAddress(0), 1);
    server.setUseClientMode(false);
    server.setNeedClientAuth(true);
    Thread serverThread =
        new Thread(
            () -> {
              while (true) {
                try {
                  logger.info("Server accepting client");
                  final SSLSocket conn = (SSLSocket) server.accept();
                  logger.info("Server accepted client {}", conn);
                  conn.addHandshakeCompletedListener(
                      event -> {
                        boolean offer;
                        try {
                          // Transfer certificates to client thread once handshake is complete, so
                          // it can safely close
                          // the socket
                          offer =
                              peerCertificates.offer(
                                  Optional.of((X509Certificate[]) event.getPeerCertificates()));
                        } catch (SSLPeerUnverifiedException e) {
                          offer = peerCertificates.offer(Optional.empty());
                        }
                        Assert.assertTrue(offer);
                      });
                  logger.info("Server starting handshake");
                  // Without this, client handshake blocks
                  conn.startHandshake();
                } catch (IOException e) {
                  // Not sure why I sometimes see ~thousands of these locally
                  if (e instanceof SocketException && e.getMessage().contains("Socket closed"))
                    return;
                  logger.info("Server accept error", e);
                }
              }
            });
    serverThread.setName(String.format("%s-serverThread", this.getClass().getSimpleName()));
    serverThread.setDaemon(true);
    serverThread.start();

    final ReloadingKeyManagerFactory kmf =
        ReloadingKeyManagerFactory.create(
            TMP_CLIENT_KEYSTORE_PATH, CERTSTORE_PASSWORD, Optional.empty());
    // Need a tmf that tells the server to send its certs
    final TrustManagerFactory tmf = buildTrustManagerFactory();

    // Check original client certificate
    testClientCertificates(
        kmf,
        tmf,
        server.getLocalSocketAddress(),
        () -> {
          try {
            return peerCertificates.poll(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        },
        certs -> {
          Assert.assertEquals(1, certs.length);
          X509Certificate cert = certs[0];
          Assert.assertEquals(ORIGINAL_CLIENT_KEYSTORE_CERT_SERIAL, cert.getSerialNumber());
        });

    // Update keystore content
    logger.info("Updating keystore file with new content");
    Files.copy(
        ALTERNATE_CLIENT_KEYSTORE_PATH,
        TMP_CLIENT_KEYSTORE_PATH,
        REPLACE_EXISTING,
        COPY_ATTRIBUTES);
    kmf.reload();

    // Check that alternate client certificate was applied
    testClientCertificates(
        kmf,
        tmf,
        server.getLocalSocketAddress(),
        () -> {
          try {
            return peerCertificates.poll(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        },
        certs -> {
          Assert.assertEquals(1, certs.length);
          X509Certificate cert = certs[0];
          Assert.assertEquals(ALTERNATE_CLIENT_KEYSTORE_CERT_SERIAL, cert.getSerialNumber());
        });

    kmf.close();
    server.close();
  }

  private static void testClientCertificates(
      KeyManagerFactory kmf,
      TrustManagerFactory tmf,
      SocketAddress serverAddress,
      Supplier<Optional<X509Certificate[]>> certsSupplier,
      Consumer<X509Certificate[]> certsConsumer)
      throws NoSuchAlgorithmException, KeyManagementException, IOException {
    SSLContext clientSslContext = SSLContext.getInstance("TLS");
    clientSslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    final SSLSocket client = (SSLSocket) clientSslContext.getSocketFactory().createSocket();
    logger.info("Client connecting");
    client.connect(serverAddress);
    logger.info("Client doing handshake");
    client.startHandshake();

    final Optional<X509Certificate[]> lastCertificate = certsSupplier.get();
    logger.info("Client got its certificate back from the server; closing socket");
    client.close();
    Assert.assertNotNull(lastCertificate);
    Assert.assertTrue(lastCertificate.isPresent());
    logger.info("Client got its certificate back from server: {}", lastCertificate);

    certsConsumer.accept(lastCertificate.get());
  }

  private static BigInteger convertSerial(String hex) {
    final BigInteger serial = new BigInteger(Integer.valueOf(hex, 16).toString());
    logger.info("Serial hex {} is {}", hex, serial);
    return serial;
  }
}
