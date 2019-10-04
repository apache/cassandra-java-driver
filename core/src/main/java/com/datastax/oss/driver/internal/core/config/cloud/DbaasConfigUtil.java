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
package com.datastax.oss.driver.internal.core.config.cloud;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class DbaasConfigUtil {

  private static final String CONFIG_FILE = "config.json";
  public static final String CONFIG_TRUSTSTORE_FILE = "trustStore.jks";
  public static final String CONFIG_KEYSTORE_FILE = "identity.jks";

  private static final String METADATA_CONTACT_INFO = "contact_info";
  private static final String METADATA_CONTACT_POINTS = "contact_points";
  private static final String METADATA_LOCAL_DC = "local_dc";
  private static final String METADATA_PROXY_ADDRESS = "sni_proxy_address";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @NonNull
  public static DbaasConfig getConfig(@NonNull URL secureConnectBundleUrl) {
    try {
      DbaasConfig config = getBaseConfig(secureConnectBundleUrl);
      return getProxyMetadata(config);
    } catch (Exception exception) {
      throw new IllegalStateException(
          "Unable to construct cloud configuration from url " + secureConnectBundleUrl, exception);
    }
  }

  @NonNull
  public static SSLContext getSSLContext(@NonNull DbaasConfig config) throws Exception {
    SSLContext context = SSLContext.getInstance("SSL");
    TrustManagerFactory tmf;
    try (InputStream trustStoreStream =
        openZippedFileInputStream(config.getSecureConnectBundleUrl(), CONFIG_TRUSTSTORE_FILE)) {
      KeyStore ts = KeyStore.getInstance("JKS");
      char[] trustPassword = config.getTrustStorePassword().toCharArray();
      ts.load(trustStoreStream, trustPassword);
      tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ts);
    }
    // initialize keystore.
    KeyManagerFactory kmf;
    try (InputStream keyStoreStream =
        openZippedFileInputStream(config.getSecureConnectBundleUrl(), CONFIG_KEYSTORE_FILE)) {
      KeyStore ks = KeyStore.getInstance("JKS");
      char[] keyStorePassword = config.getKeyStorePassword().toCharArray();
      ks.load(keyStoreStream, keyStorePassword);
      kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, keyStorePassword);
    }
    context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
    return context;
  }

  @VisibleForTesting
  @NonNull
  static DbaasConfig getBaseConfig(@NonNull URL secureConnectBundleUrl) throws Exception {
    try (InputStream jsonConfigInputStream =
        openZippedFileInputStream(secureConnectBundleUrl, CONFIG_FILE)) {
      ObjectMapper mapper = new ObjectMapper();
      DbaasConfig config = mapper.readValue(jsonConfigInputStream, DbaasConfig.class);
      config.setSecureConnectBundleUrl(secureConnectBundleUrl);
      return config;
    }
  }

  @NonNull
  private static InputStream openZippedFileInputStream(
      @NonNull URL zipFileUrl, @NonNull String innerFileName) throws IOException {
    ZipInputStream zipInputStream = new ZipInputStream(zipFileUrl.openStream());
    ZipEntry entry;
    while ((entry = zipInputStream.getNextEntry()) != null) {
      if (entry.getName().equals(innerFileName)) {
        return zipInputStream;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Unable to find innerFileName: %s in the zipFileUrl: %s", innerFileName, zipFileUrl));
  }

  @NonNull
  private static DbaasConfig getProxyMetadata(@NonNull DbaasConfig dbaasConfig) throws Exception {
    SSLContext sslContext = getSSLContext(dbaasConfig);
    StringBuilder result = new StringBuilder();
    URL metaDataServiceUrl =
        new URL("https://" + dbaasConfig.getHost() + ":" + dbaasConfig.getPort() + "/metadata");
    HttpsURLConnection connection = (HttpsURLConnection) metaDataServiceUrl.openConnection();
    connection.setSSLSocketFactory(sslContext.getSocketFactory());
    connection.setRequestMethod("GET");
    connection.setRequestProperty("host", "localhost");
    try (BufferedReader rd =
        new BufferedReader(
            new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {

      String line;
      while ((line = rd.readLine()) != null) {
        result.append(line);
      }
    }
    return getConfigFromMetadataJson(dbaasConfig, result.toString());
  }

  @VisibleForTesting
  @NonNull
  static DbaasConfig getConfigFromMetadataJson(
      @NonNull DbaasConfig dbaasConfig, @NonNull String jsonString) throws Exception {
    JsonNode json = OBJECT_MAPPER.readTree(jsonString);
    JsonNode contactInfo = json.get(METADATA_CONTACT_INFO);
    dbaasConfig.setLocalDC(contactInfo.get(METADATA_LOCAL_DC).asText());
    List<String> hostIds = new ArrayList<>();
    JsonNode hostIdsJSON = contactInfo.get(METADATA_CONTACT_POINTS);
    for (int i = 0; i < hostIdsJSON.size(); i++) {
      hostIds.add(hostIdsJSON.get(i).asText());
    }
    dbaasConfig.setHostIds(hostIds);
    List<String> sniHostComplete =
        Splitter.on(":").splitToList(contactInfo.get(METADATA_PROXY_ADDRESS).asText());
    dbaasConfig.setSniHost(sniHostComplete.get(0));
    dbaasConfig.setSniPort(Integer.parseInt(sniHostComplete.get(1)));
    return dbaasConfig;
  }
}
