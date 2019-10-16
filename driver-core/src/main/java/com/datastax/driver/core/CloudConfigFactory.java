/*
 * Copyright DataStax, Inc.
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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.net.HostAndPort;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

class CloudConfigFactory {

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
  CloudConfig createCloudConfig(InputStream cloudConfig)
      throws IOException, GeneralSecurityException {
    checkNotNull(cloudConfig, "cloudConfig cannot be null");
    JsonNode configJson = null;
    ByteArrayOutputStream keyStoreOutputStream = null;
    ByteArrayOutputStream trustStoreOutputStream = null;
    ObjectMapper mapper = new ObjectMapper().configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ZipInputStream zipInputStream = null;
    try {
      zipInputStream = new ZipInputStream(cloudConfig);
      ZipEntry entry;
      while ((entry = zipInputStream.getNextEntry()) != null) {
        String fileName = entry.getName();
        if (fileName.equals("config.json")) {
          configJson = mapper.readTree(zipInputStream);
        } else if (fileName.equals("identity.jks")) {
          keyStoreOutputStream = new ByteArrayOutputStream();
          ByteStreams.copy(zipInputStream, keyStoreOutputStream);
        } else if (fileName.equals("trustStore.jks")) {
          trustStoreOutputStream = new ByteArrayOutputStream();
          ByteStreams.copy(zipInputStream, trustStoreOutputStream);
        }
      }
    } finally {
      if (zipInputStream != null) {
        zipInputStream.close();
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
    BufferedReader proxyMetadata = null;
    try {
      proxyMetadata = fetchProxyMetadata(metadataServiceUrl, sslContext);
      proxyMetadataJson = mapper.readTree(proxyMetadata);
    } finally {
      if (proxyMetadata != null) {
        proxyMetadata.close();
      }
    }
    InetSocketAddress sniProxyAddress = getSniProxyAddress(proxyMetadataJson);
    List<EndPoint> endPoints = getEndPoints(proxyMetadataJson, sniProxyAddress);
    String localDatacenter = getLocalDatacenter(proxyMetadataJson);
    SSLOptions sslOptions = getSSLOptions(sslContext);
    AuthProvider authProvider = getAuthProvider(configJson);
    return new CloudConfig(sniProxyAddress, endPoints, localDatacenter, sslOptions, authProvider);
  }

  protected char[] getKeyStorePassword(JsonNode configFile) {
    if (configFile.has("keyStorePassword")) {
      return configFile.get("keyStorePassword").asText().toCharArray();
    } else {
      throw new IllegalStateException("Invalid config.json: missing field keyStorePassword");
    }
  }

  protected char[] getTrustStorePassword(JsonNode configFile) {
    if (configFile.has("trustStorePassword")) {
      return configFile.get("trustStorePassword").asText().toCharArray();
    } else {
      throw new IllegalStateException("Invalid config.json: missing field trustStorePassword");
    }
  }

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

  protected AuthProvider getAuthProvider(JsonNode configFile) {
    if (configFile.has("username")) {
      String username = configFile.get("username").asText();
      if (configFile.has("password")) {
        String password = configFile.get("password").asText();
        return new PlainTextAuthProvider(username, password);
      }
    }
    return null;
  }

  protected SSLContext createSslContext(
      ByteArrayInputStream keyStoreInputStream,
      char[] keyStorePassword,
      ByteArrayInputStream trustStoreInputStream,
      char[] trustStorePassword)
      throws IOException, GeneralSecurityException {
    KeyManagerFactory kmf = createKeyManagerFactory(keyStoreInputStream, keyStorePassword);
    TrustManagerFactory tmf = createTrustManagerFactory(trustStoreInputStream, trustStorePassword);
    SSLContext sslContext = SSLContext.getInstance("SSL");
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
    return sslContext;
  }

  protected KeyManagerFactory createKeyManagerFactory(
      InputStream keyStoreInputStream, char[] keyStorePassword)
      throws IOException, GeneralSecurityException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(keyStoreInputStream, keyStorePassword);
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, keyStorePassword);
    Arrays.fill(keyStorePassword, (char) 0);
    return kmf;
  }

  protected TrustManagerFactory createTrustManagerFactory(
      InputStream trustStoreInputStream, char[] trustStorePassword)
      throws IOException, GeneralSecurityException {
    KeyStore ts = KeyStore.getInstance("JKS");
    ts.load(trustStoreInputStream, trustStorePassword);
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);
    Arrays.fill(trustStorePassword, (char) 0);
    return tmf;
  }

  protected BufferedReader fetchProxyMetadata(URL metadataServiceUrl, SSLContext sslContext)
      throws IOException {
    HttpsURLConnection connection = (HttpsURLConnection) metadataServiceUrl.openConnection();
    connection.setSSLSocketFactory(sslContext.getSocketFactory());
    connection.setRequestMethod("GET");
    connection.setRequestProperty("host", "localhost");
    return new BufferedReader(new InputStreamReader(connection.getInputStream(), UTF_8));
  }

  protected String getLocalDatacenter(JsonNode proxyMetadata) {
    JsonNode contactInfo = getContactInfo(proxyMetadata);
    if (contactInfo.has("local_dc")) {
      return contactInfo.get("local_dc").asText();
    } else {
      throw new IllegalStateException("Invalid proxy metadata: missing field local_dc");
    }
  }

  protected InetSocketAddress getSniProxyAddress(JsonNode proxyMetadata) {
    JsonNode contactInfo = getContactInfo(proxyMetadata);
    if (contactInfo.has("sni_proxy_address")) {
      HostAndPort sniProxyHostAndPort =
          HostAndPort.fromString(contactInfo.get("sni_proxy_address").asText());
      if (!sniProxyHostAndPort.hasPort()) {
        throw new IllegalStateException(
            "Invalid proxy metadata: missing port from field sni_proxy_address");
      }
      return InetSocketAddress.createUnresolved(
          sniProxyHostAndPort.getHostText(), sniProxyHostAndPort.getPort());
    } else {
      throw new IllegalStateException("Invalid proxy metadata: missing field sni_proxy_address");
    }
  }

  protected List<EndPoint> getEndPoints(JsonNode proxyMetadata, InetSocketAddress sniProxyAddress) {
    JsonNode contactInfo = getContactInfo(proxyMetadata);
    if (contactInfo.has("contact_points")) {
      List<EndPoint> endPoints = new ArrayList<EndPoint>();
      JsonNode hostIdsJson = contactInfo.get("contact_points");
      for (int i = 0; i < hostIdsJson.size(); i++) {
        endPoints.add(new SniEndPoint(sniProxyAddress, hostIdsJson.get(i).asText()));
      }
      return endPoints;
    } else {
      throw new IllegalStateException("Invalid proxy metadata: missing field contact_points");
    }
  }

  protected JsonNode getContactInfo(JsonNode proxyMetadata) {
    if (proxyMetadata.has("contact_info")) {
      return proxyMetadata.get("contact_info");
    } else {
      throw new IllegalStateException("Invalid proxy metadata: missing field contact_info");
    }
  }

  protected SSLOptions getSSLOptions(SSLContext sslContext) {
    try {
      return SniSSLOptions.builder().withSSLContext(sslContext).build();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
