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

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.internal.core.ssl.SniSslEngineFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.github.tomakehurst.wiremock.common.JettySettings;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.http.AdminRequestHandler;
import com.github.tomakehurst.wiremock.http.HttpServer;
import com.github.tomakehurst.wiremock.http.HttpServerFactory;
import com.github.tomakehurst.wiremock.http.StubRequestHandler;
import com.github.tomakehurst.wiremock.jetty9.JettyHttpServer;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.base.Joiner;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.eclipse.jetty.io.NetworkTrafficListener;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CloudConfigFactoryTest {

  private static final String BUNDLE_PATH = "/config/cloud/creds.zip";

  @Rule
  public WireMockRule wireMockRule =
      new WireMockRule(
          wireMockConfig()
              .httpsPort(30443)
              .dynamicPort()
              .httpServerFactory(new HttpsServerFactory())
              .needClientAuth(true)
              .keystorePath(path("/config/cloud/identity.jks").toString())
              .keystorePassword("fakePasswordForTests")
              .trustStorePath(path("/config/cloud/trustStore.jks").toString())
              .trustStorePassword("fakePasswordForTests2"));

  public CloudConfigFactoryTest() throws URISyntaxException {}

  @Test
  public void should_load_config_from_local_filesystem() throws Exception {
    // given
    URL configFile = getClass().getResource(BUNDLE_PATH);
    mockProxyMetadataService(jsonMetadata());
    // when
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    CloudConfig cloudConfig = cloudConfigFactory.createCloudConfig(configFile);
    // then
    assertCloudConfig(cloudConfig);
  }

  @Test
  public void should_load_config_from_external_location() throws Exception {
    // given
    mockHttpSecureBundle(secureBundle());
    mockProxyMetadataService(jsonMetadata());
    // when
    URL configFile = new URL("http", "localhost", wireMockRule.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    CloudConfig cloudConfig = cloudConfigFactory.createCloudConfig(configFile);
    // then
    assertCloudConfig(cloudConfig);
  }

  @Test
  public void should_throw_when_bundle_not_found() throws Exception {
    // given
    stubFor(any(urlEqualTo(BUNDLE_PATH)).willReturn(aResponse().withStatus(404)));
    // when
    URL configFile = new URL("http", "localhost", wireMockRule.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    Throwable t = catchThrowable(() -> cloudConfigFactory.createCloudConfig(configFile));
    assertThat(t)
        .isInstanceOf(FileNotFoundException.class)
        .hasMessageContaining(configFile.toExternalForm());
  }

  @Test
  public void should_throw_when_bundle_not_readable() throws Exception {
    // given
    mockHttpSecureBundle("not a zip file".getBytes(StandardCharsets.UTF_8));
    // when
    URL configFile = new URL("http", "localhost", wireMockRule.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    Throwable t = catchThrowable(() -> cloudConfigFactory.createCloudConfig(configFile));
    assertThat(t)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Invalid bundle: missing file config.json");
  }

  @Test
  public void should_throw_when_metadata_not_found() throws Exception {
    // given
    mockHttpSecureBundle(secureBundle());
    stubFor(any(urlPathEqualTo("/metadata")).willReturn(aResponse().withStatus(404)));
    // when
    URL configFile = new URL("http", "localhost", wireMockRule.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    Throwable t = catchThrowable(() -> cloudConfigFactory.createCloudConfig(configFile));
    assertThat(t).isInstanceOf(FileNotFoundException.class).hasMessageContaining("metadata");
  }

  @Test
  public void should_throw_when_metadata_not_readable() throws Exception {
    // given
    mockHttpSecureBundle(secureBundle());
    mockProxyMetadataService("not a valid json payload");
    // when
    URL configFile = new URL("http", "localhost", wireMockRule.port(), BUNDLE_PATH);
    CloudConfigFactory cloudConfigFactory = new CloudConfigFactory();
    Throwable t = catchThrowable(() -> cloudConfigFactory.createCloudConfig(configFile));
    assertThat(t).isInstanceOf(JsonParseException.class).hasMessageContaining("Unrecognized token");
  }

  private void mockHttpSecureBundle(byte[] body) {
    stubFor(
        any(urlEqualTo(BUNDLE_PATH))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(body)));
  }

  private void mockProxyMetadataService(String jsonMetadata) {
    stubFor(
        any(urlPathEqualTo("/metadata"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(jsonMetadata)));
  }

  private byte[] secureBundle() throws IOException, URISyntaxException {
    return Files.readAllBytes(path(BUNDLE_PATH));
  }

  private String jsonMetadata() throws IOException, URISyntaxException {
    return Joiner.on('\n')
        .join(Files.readAllLines(path("/config/cloud/metadata.json"), StandardCharsets.UTF_8));
  }

  private Path path(String resource) throws URISyntaxException {
    return Paths.get(getClass().getResource(resource).toURI());
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
    assertThat(config.getSslEngineFactory()).isNotNull().isInstanceOf(SniSslEngineFactory.class);
  }

  static {
    javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
        (hostname, sslSession) -> hostname.equals("localhost"));
  }

  // see https://github.com/tomakehurst/wiremock/issues/874
  private static class HttpsServerFactory implements HttpServerFactory {
    @Override
    public HttpServer buildHttpServer(
        Options options,
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
