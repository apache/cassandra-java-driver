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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DbaasConfigUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(DbaasConfigUtil.class);
  private static final String CONFIG_FILE = "config.json";
  private static final String CONFIG_TRUSTSTORE_FILE = "trustStore.jks";
  private static final String CONFIG_KEYSTORE_FILE = "identity.jks";

  private static final String METADATA_CONTACT_INFO = "contact_info";
  private static final String METADATA_CONTACT_POINTS = "contact_points";
  private static final String METADATA_LOCAL_DC = "local_dc";
  private static final String METADATA_PROXY_ADDRESS = "sni_proxy_address";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static DbaasConfiguration getConfig(String secureBundlePath) {
    try {
      File secureBundleFile = new File(secureBundlePath);
      DbaasConfiguration dbaasConfig = getBaseConfig(secureBundleFile);
      dbaasConfig = getProxyMetadata(dbaasConfig);
      return dbaasConfig;
    } catch (Exception exception) {
      throw new IllegalStateException(
          "Unable to construct cloud configuration from url " + secureBundlePath, exception);
    }
  }

  static DbaasConfiguration getBaseConfig(File secureBundleFile) throws Exception {

    InputStream jsonInputStream = getJsonConfigInputStream(secureBundleFile);
    try {
      ObjectMapper mapper = new ObjectMapper();
      DbaasConfiguration config = mapper.readValue(jsonInputStream, DbaasConfiguration.class);
      config.setSecureBundlePath(secureBundleFile.toString());
      return config;
    } finally {
      jsonInputStream.close();
    }
  }

  private static InputStream getJsonConfigInputStream(File secureBundleFile) throws IOException {
    return getZippedFile(secureBundleFile, CONFIG_FILE);
  }

  private static InputStream getZippedFile(File secureBundleFile, String innerFileName)
      throws IOException {
    ZipFile zipFile = new ZipFile(secureBundleFile);
    ZipEntry configEntry = zipFile.getEntry(innerFileName);
    return zipFile.getInputStream(configEntry);
  }

  private static InputStream getZippedFile(String secureBundlePath, String innerFileName)
      throws IOException {
    return getZippedFile(new File(secureBundlePath), innerFileName);
  }

  private static InputStream getTrustStore(DbaasConfiguration config) throws IOException {
    return getZippedFile(config.getSecureBundlePath(), CONFIG_TRUSTSTORE_FILE);
  }

  private static InputStream getKeyStore(DbaasConfiguration config) throws IOException {
    return getZippedFile(config.getSecureBundlePath(), CONFIG_KEYSTORE_FILE);
  }

  private static DbaasConfiguration getProxyMetadata(DbaasConfiguration dbaasConfig)
      throws Exception {
    try {
      URL metaDataServiceUrl =
          new URL("https://" + dbaasConfig.getHost() + ":" + dbaasConfig.getPort() + "/metadata");
      HttpsURLConnection connection = (HttpsURLConnection) metaDataServiceUrl.openConnection();
      SSLContext sslContext = DbaasConfigUtil.getSSLContext(dbaasConfig);
      connection.setSSLSocketFactory(sslContext.getSocketFactory());
      connection.setRequestMethod("GET");
      connection.setRequestProperty("host", "localhost");
      BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      StringBuilder result = new StringBuilder();
      try {
        String line;
        while ((line = rd.readLine()) != null) {
          result.append(line);
        }
      } finally {
        rd.close();
      }
      return getConfigFromMetadataJson(dbaasConfig, result.toString());
    } catch (Exception e) {
      LOGGER.error("Unable to fetch cluster metadata", e);
      throw e;
    }
  }

  static DbaasConfiguration getConfigFromMetadataJson(
      DbaasConfiguration dbaasConfig, String jsonString) throws Exception {
    JsonNode json = OBJECT_MAPPER.readTree(jsonString);
    JsonNode contactInfo = json.get(METADATA_CONTACT_INFO);
    dbaasConfig.setLocalDC(contactInfo.get(METADATA_LOCAL_DC).asText());
    List<String> hostIds = new ArrayList<String>();
    JsonNode hostIdsJSON = contactInfo.get(METADATA_CONTACT_POINTS);
    for (int i = 0; i < hostIdsJSON.size(); i++) {
      hostIds.add(hostIdsJSON.get(i).asText());
    }
    dbaasConfig.setHostIds(hostIds);
    String[] sniHostComplete = contactInfo.get(METADATA_PROXY_ADDRESS).asText().split(":");
    dbaasConfig.setSniHost(sniHostComplete[0]);
    dbaasConfig.setSniPort(Integer.parseInt(sniHostComplete[1]));
    return dbaasConfig;
  }

  private static SSLContext getSSLContext(DbaasConfiguration config) throws Exception {

    TrustManagerFactory tmf;
    SSLContext context = SSLContext.getInstance("SSL");
    InputStream trustStoreStream = getTrustStore(config);
    try {
      KeyStore ts = KeyStore.getInstance("JKS");
      char[] trustPassword = config.getTrustStorePassword().toCharArray();
      ts.load(trustStoreStream, trustPassword);
      tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ts);
    } finally {
      trustStoreStream.close();
    }

    // initialize keystore.
    KeyManagerFactory kmf;
    InputStream keyStoreStream = getKeyStore(config);
    try {
      KeyStore ks = KeyStore.getInstance("JKS");
      char[] keyStorePassword = config.getKeyStorePassword().toCharArray();
      ks.load(keyStoreStream, keyStorePassword);
      kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, keyStorePassword);
    } finally {
      keyStoreStream.close();
    }

    context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
    return context;
  }

  static SSLOptions getSSLOptions(DbaasConfiguration dbaasConfig) {
    try {
      return SniSSLOptions.builder().withSSLContext(getSSLContext(dbaasConfig)).build();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
