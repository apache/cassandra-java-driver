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
package com.datastax.oss.driver.internal.core.config.cloud;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;
import com.datastax.oss.driver.internal.core.ssl.SniSslEngineFactory;
import com.datastax.oss.driver.shaded.guava.common.io.ByteStreams;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CloudConfigFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CloudConfigFactory.class);
  /**
   * Creates a {@link CloudConfig} with information fetched from the specified Cloud configuration
   * URL.
   *
   * <p>The target URL must point to a valid secure connect bundle archive in ZIP format.
   *
   * @param cloudConfigUrl the URL to fetch the Cloud configuration from; cannot be null.
   * @throws IOException If the Cloud configuration cannot be read.
   * @throws GeneralSecurityException If the Cloud SSL context cannot be created.
   */
  @NonNull
  public CloudConfig createCloudConfig(@NonNull URL cloudConfigUrl)
      throws IOException, GeneralSecurityException {
    Objects.requireNonNull(cloudConfigUrl, "cloudConfigUrl cannot be null");
    return createCloudConfig(cloudConfigUrl.openStream());
  }

  /**
   * Creates a {@link CloudConfig} with information fetched from the specified {@link InputStream}.
   *
   * <p>The stream must contain a valid secure connect bundle archive in ZIP format. Note that the
   * stream will be closed after a call to that method and cannot be used anymore.
   *
   * @param cloudConfig the stream to read the Cloud configuration from; cannot be null.
   * @throws IOException If the Cloud configuration cannot be read.
   * @throws GeneralSecurityException If the Cloud SSL context cannot be created.
   */
  @NonNull
  public CloudConfig createCloudConfig(@NonNull InputStream cloudConfig)
      throws IOException, GeneralSecurityException {
    Objects.requireNonNull(cloudConfig, "cloudConfig cannot be null");
    JsonNode configJson = null;
    ByteArrayOutputStream keyStoreOutputStream = null;
    ByteArrayOutputStream trustStoreOutputStream = null;
    ObjectMapper mapper = new ObjectMapper().configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    try (ZipInputStream zipInputStream = new ZipInputStream(cloudConfig)) {
      ZipEntry entry;
      while ((entry = zipInputStream.getNextEntry()) != null) {
        String fileName = entry.getName();
        switch (fileName) {
          case "config.json":
            configJson = mapper.readTree(zipInputStream);
            break;
          case "identity.jks":
            keyStoreOutputStream = new ByteArrayOutputStream();
            ByteStreams.copy(zipInputStream, keyStoreOutputStream);
            break;
          case "trustStore.jks":
            trustStoreOutputStream = new ByteArrayOutputStream();
            ByteStreams.copy(zipInputStream, trustStoreOutputStream);
            break;
        }
      }
    }
    if (configJson == null) {
      throw new IllegalStateException("Invalid bundle: missing file config.json");
    }
    if (keyStoreOutputStream == null) {
      throw new IllegalStateException("Invalid bundle: missing file identity.jks");
    }
    if (trustStoreOutputStream == null) {
      throw new IllegalStateException("Invalid bundle: missing file trustStore.jks");
    }
    char[] keyStorePassword = getKeyStorePassword(configJson);
    char[] trustStorePassword = getTrustStorePassword(configJson);
    ByteArrayInputStream keyStoreInputStream =
        new ByteArrayInputStream(keyStoreOutputStream.toByteArray());
    ByteArrayInputStream trustStoreInputStream =
        new ByteArrayInputStream(trustStoreOutputStream.toByteArray());
    SSLContext sslContext =
        createSslContext(
            keyStoreInputStream, keyStorePassword, trustStoreInputStream, trustStorePassword);
    URL metadataServiceUrl = getMetadataServiceUrl(configJson);
    JsonNode proxyMetadataJson;
    try (BufferedReader proxyMetadata = fetchProxyMetadata(metadataServiceUrl, sslContext)) {
      proxyMetadataJson = mapper.readTree(proxyMetadata);
    }
    InetSocketAddress sniProxyAddress = getSniProxyAddress(proxyMetadataJson);
    List<EndPoint> endPoints = getEndPoints(proxyMetadataJson, sniProxyAddress);
    String localDatacenter = getLocalDatacenter(proxyMetadataJson);
    SniSslEngineFactory sslEngineFactory = new SniSslEngineFactory(sslContext);
    validateIfBundleContainsUsernamePassword(configJson);
    return new CloudConfig(sniProxyAddress, endPoints, localDatacenter, sslEngineFactory);
  }

  @NonNull
  protected char[] getKeyStorePassword(JsonNode configFile) {
    if (configFile.has("keyStorePassword")) {
      return configFile.get("keyStorePassword").asText().toCharArray();
    } else {
      throw new IllegalStateException("Invalid config.json: missing field keyStorePassword");
    }
  }

  @NonNull
  protected char[] getTrustStorePassword(JsonNode configFile) {
    if (configFile.has("trustStorePassword")) {
      return configFile.get("trustStorePassword").asText().toCharArray();
    } else {
      throw new IllegalStateException("Invalid config.json: missing field trustStorePassword");
    }
  }

  @NonNull
  protected URL getMetadataServiceUrl(JsonNode configFile) throws MalformedURLException {
    if (configFile.has("host")) {
      String metadataServiceHost = configFile.get("host").asText();
      if (configFile.has("port")) {
        int metadataServicePort = configFile.get("port").asInt();
        return new URL("https", metadataServiceHost, metadataServicePort, "/metadata");
      } else {
        throw new IllegalStateException("Invalid config.json: missing field port");
      }
    } else {
      throw new IllegalStateException("Invalid config.json: missing field host");
    }
  }

  protected void validateIfBundleContainsUsernamePassword(JsonNode configFile) {
    if (configFile.has("username") || configFile.has("password")) {
      LOG.info(
          "The bundle contains config.json with username and/or password. Providing it in the bundle is deprecated and ignored.");
    }
  }

  @NonNull
  protected SSLContext createSslContext(
      @NonNull ByteArrayInputStream keyStoreInputStream,
      @NonNull char[] keyStorePassword,
      @NonNull ByteArrayInputStream trustStoreInputStream,
      @NonNull char[] trustStorePassword)
      throws IOException, GeneralSecurityException {
    KeyManagerFactory kmf = createKeyManagerFactory(keyStoreInputStream, keyStorePassword);
    TrustManagerFactory tmf = createTrustManagerFactory(trustStoreInputStream, trustStorePassword);
    SSLContext sslContext = SSLContext.getInstance("SSL");
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
    return sslContext;
  }

  @NonNull
  protected KeyManagerFactory createKeyManagerFactory(
      @NonNull InputStream keyStoreInputStream, @NonNull char[] keyStorePassword)
      throws IOException, GeneralSecurityException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(keyStoreInputStream, keyStorePassword);
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, keyStorePassword);
    Arrays.fill(keyStorePassword, (char) 0);
    return kmf;
  }

  @NonNull
  protected TrustManagerFactory createTrustManagerFactory(
      @NonNull InputStream trustStoreInputStream, @NonNull char[] trustStorePassword)
      throws IOException, GeneralSecurityException {
    KeyStore ts = KeyStore.getInstance("JKS");
    ts.load(trustStoreInputStream, trustStorePassword);
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);
    Arrays.fill(trustStorePassword, (char) 0);
    return tmf;
  }

  @NonNull
  protected BufferedReader fetchProxyMetadata(
      @NonNull URL metadataServiceUrl, @NonNull SSLContext sslContext) throws IOException {
    try {
      HttpsURLConnection connection = (HttpsURLConnection) metadataServiceUrl.openConnection();
      connection.setSSLSocketFactory(sslContext.getSocketFactory());
      connection.setRequestMethod("GET");
      connection.setRequestProperty("host", "localhost");
      return new BufferedReader(
          new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
    } catch (ConnectException e) {
      throw new IllegalStateException(
          "Unable to connect to cloud metadata service. Please make sure your cluster is not parked or terminated",
          e);
    } catch (UnknownHostException e) {
      throw new IllegalStateException(
          "Unable to resolve host for cloud metadata service. Please make sure your cluster is not terminated",
          e);
    }
  }

  @NonNull
  protected String getLocalDatacenter(@NonNull JsonNode proxyMetadata) {
    JsonNode contactInfo = getContactInfo(proxyMetadata);
    if (contactInfo.has("local_dc")) {
      return contactInfo.get("local_dc").asText();
    } else {
      throw new IllegalStateException("Invalid proxy metadata: missing field local_dc");
    }
  }

  @NonNull
  protected InetSocketAddress getSniProxyAddress(@NonNull JsonNode proxyMetadata) {
    JsonNode contactInfo = getContactInfo(proxyMetadata);
    if (contactInfo.has("sni_proxy_address")) {
      HostAndPort sniProxyHostAndPort =
          HostAndPort.fromString(contactInfo.get("sni_proxy_address").asText());
      if (!sniProxyHostAndPort.hasPort()) {
        throw new IllegalStateException(
            "Invalid proxy metadata: missing port from field sni_proxy_address");
      }
      return InetSocketAddress.createUnresolved(
          sniProxyHostAndPort.getHost(), sniProxyHostAndPort.getPort());
    } else {
      throw new IllegalStateException("Invalid proxy metadata: missing field sni_proxy_address");
    }
  }

  @NonNull
  protected List<EndPoint> getEndPoints(
      @NonNull JsonNode proxyMetadata, @NonNull InetSocketAddress sniProxyAddress) {
    JsonNode contactInfo = getContactInfo(proxyMetadata);
    if (contactInfo.has("contact_points")) {
      List<EndPoint> endPoints = new ArrayList<>();
      JsonNode hostIdsJson = contactInfo.get("contact_points");
      for (int i = 0; i < hostIdsJson.size(); i++) {
        endPoints.add(new SniEndPoint(sniProxyAddress, hostIdsJson.get(i).asText()));
      }
      return endPoints;
    } else {
      throw new IllegalStateException("Invalid proxy metadata: missing field contact_points");
    }
  }

  @NonNull
  protected JsonNode getContactInfo(@NonNull JsonNode proxyMetadata) {
    if (proxyMetadata.has("contact_info")) {
      return proxyMetadata.get("contact_info");
    } else {
      throw new IllegalStateException("Invalid proxy metadata: missing field contact_info");
    }
  }
}
