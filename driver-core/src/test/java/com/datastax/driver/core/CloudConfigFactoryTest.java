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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.apache.commons.codec.CharEncoding.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.JettySettings;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.http.AdminRequestHandler;
import com.github.tomakehurst.wiremock.http.HttpServer;
import com.github.tomakehurst.wiremock.http.HttpServerFactory;
import com.github.tomakehurst.wiremock.http.StubRequestHandler;
import com.github.tomakehurst.wiremock.jetty9.JettyHttpServer;
import com.google.common.base.Joiner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import org.apache.commons.io.FileUtils;
import org.eclipse.jetty.io.NetworkTrafficListener;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CloudConfigFactoryTest {

  private static final String BUNDLE_PATH = "/cloud/creds.zip";

  private WireMockServer wireMockServer;

  @BeforeMethod
  public void startWireMock() {
    wireMockServer =
        new WireMockServer(
            wireMockConfig()
                .httpsPort(30443)
                .dynamicPort()
                .httpServerFactory(new HttpsServerFactory())
                .needClientAuth(true)
                .keystorePath(path("/cloud/identity.jks"))
                .keystorePassword("XS78x3GuBWas1OoA5")
                .trustStorePath(path("/cloud/trustStore.jks"))
                .trustStorePassword("48ZY5r06BmpVLKxPg"));
    wireMockServer.start();
  }

  @AfterMethod
  public void stopWireMock() {
    wireMockServer.stop();
  }

  @Test
  public void should_load_config_from_local_filesystem() throws Exception {
    // given
    URL configFile = getClass().getResource(BUNDLE_PATH);
    mockProxyMetadataService(jsonMetadata());
    // when
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    CloudConfig cloudConfig = cloudConfigFactory.createCloudConfig(configFile.openStream());
    // then
    assertCloudConfig(cloudConfig);
  }

  @Test
  public void should_load_config_from_external_location() throws Exception {
    // given
    mockHttpSecureBundle(secureBundle());
    mockProxyMetadataService(jsonMetadata());
    // when
    URL configFile = new URL("http", "localhost", wireMockServer.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    CloudConfig cloudConfig = cloudConfigFactory.createCloudConfig(configFile.openStream());
    // then
    assertCloudConfig(cloudConfig);
  }

  @Test
  public void should_throw_when_bundle_not_found() throws Exception {
    // given
    wireMockServer.stubFor(any(urlEqualTo(BUNDLE_PATH)).willReturn(aResponse().withStatus(404)));
    // when
    URL configFile = new URL("http", "localhost", wireMockServer.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    try {
      cloudConfigFactory.createCloudConfig(configFile.openStream());
    } catch (FileNotFoundException ex) {
      assertThat(ex).hasMessageContaining(configFile.toExternalForm());
    }
  }

  @Test
  public void should_throw_when_bundle_not_readable() throws Exception {
    // given
    mockHttpSecureBundle("not a zip file".getBytes(UTF_8));
    // when
    URL configFile = new URL("http", "localhost", wireMockServer.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    try {
      cloudConfigFactory.createCloudConfig(configFile.openStream());
    } catch (IllegalStateException ex) {
      assertThat(ex).hasMessageContaining("Invalid bundle: missing file config.json");
    }
  }

  @Test
  public void should_throw_when_metadata_not_found() throws Exception {
    // given
    mockHttpSecureBundle(secureBundle());
    wireMockServer.stubFor(
        any(urlPathEqualTo("/metadata")).willReturn(aResponse().withStatus(404)));
    // when
    URL configFile = new URL("http", "localhost", wireMockServer.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    try {
      cloudConfigFactory.createCloudConfig(configFile.openStream());
    } catch (FileNotFoundException ex) {
      assertThat(ex).hasMessageContaining("metadata");
    }
  }

  @Test
  public void should_throw_when_metadata_not_readable() throws Exception {
    // given
    mockHttpSecureBundle(secureBundle());
    mockProxyMetadataService("not a valid json payload");
    // when
    URL configFile = new URL("http", "localhost", wireMockServer.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    try {
      cloudConfigFactory.createCloudConfig(configFile.openStream());
    } catch (JsonParseException ex) {
      assertThat(ex).hasMessageContaining("Unrecognized token");
    }
  }

  private void mockHttpSecureBundle(byte[] body) {
    wireMockServer.stubFor(
        any(urlEqualTo(BUNDLE_PATH))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(body)));
  }

  private void mockProxyMetadataService(String jsonMetadata) {
    wireMockServer.stubFor(
        any(urlPathEqualTo("/metadata"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(jsonMetadata)));
  }

  private byte[] secureBundle() throws IOException {
    return FileUtils.readFileToByteArray(new File(path(BUNDLE_PATH)));
  }

  private String jsonMetadata() throws IOException {
    return Joiner.on('\n').join(FileUtils.readLines(new File(path("/cloud/metadata.json"))));
  }

  private String path(String resource) {
    return getClass().getResource(resource).getFile();
  }

  private void assertCloudConfig(CloudConfig config) {
    InetSocketAddress expectedProxyAddress = InetSocketAddress.createUnresolved("localhost", 30002);
    assertThat(config.getLocalDatacenter()).isEqualTo("dc1");
    assertThat(config.getProxyAddress()).isEqualTo(expectedProxyAddress);
    assertThat(config.getEndPoints()).extracting("proxyAddress").containsOnly(expectedProxyAddress);
    assertThat(config.getEndPoints())
        .extracting("serverName")
        .containsExactly(
            "4ac06655-f861-49f9-881e-3fee22e69b94",
            "2af7c253-3394-4a0d-bfac-f1ad81b5154d",
            "b17b6e2a-3f48-4d6a-81c1-20a0a1f3192a");
    assertThat(config.getSslOptions()).isNotNull().isInstanceOf(SSLOptions.class);
  }

  static {
    javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
        new HostnameVerifier() {
          @Override
          public boolean verify(String hostname, SSLSession sslSession) {
            return hostname.equals("localhost");
          }
        });
  }

  // see https://github.com/tomakehurst/wiremock/issues/874
  private static class HttpsServerFactory implements HttpServerFactory {
    @Override
    public HttpServer buildHttpServer(
        final Options options,
        AdminRequestHandler adminRequestHandler,
        StubRequestHandler stubRequestHandler) {
      return new JettyHttpServer(options, adminRequestHandler, stubRequestHandler) {
        @Override
        protected ServerConnector createServerConnector(
            String bindAddress,
            JettySettings jettySettings,
            int port,
            NetworkTrafficListener listener,
            ConnectionFactory... connectionFactories) {
          if (port == options.httpsSettings().port()) {
            SslConnectionFactory sslConnectionFactory =
                (SslConnectionFactory) connectionFactories[0];
            SslContextFactory sslContextFactory = sslConnectionFactory.getSslContextFactory();
            sslContextFactory.setKeyStorePassword(options.httpsSettings().keyStorePassword());
            connectionFactories =
                new ConnectionFactory[] {sslConnectionFactory, connectionFactories[1]};
          }
          return super.createServerConnector(
              bindAddress, jettySettings, port, listener, connectionFactories);
        }
      };
    }
  }
}
