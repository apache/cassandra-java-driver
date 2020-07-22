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
package com.datastax.oss.driver.api.core.cloud;

import static com.datastax.oss.driver.internal.core.util.LoggerTest.setupTestLogger;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IsolatedTests.class)
public class CloudIT {

  private static final String BUNDLE_URL_PATH = "/certs/bundles/creds.zip";

  @ClassRule public static SniProxyRule proxyRule = new SniProxyRule();

  // Used only to host the secure connect bundle, for tests that require external URLs
  @Rule
  public WireMockRule wireMockRule =
      new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());

  @Test
  public void should_connect_to_proxy_using_path() {
    ResultSet set;
    Path bundle = proxyRule.getProxy().getDefaultBundlePath();
    try (CqlSession session = CqlSession.builder().withCloudSecureConnectBundle(bundle).build()) {
      set = session.execute("select * from system.local");
    }
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_without_credentials() {
    ResultSet set;
    Path bundle = proxyRule.getProxy().getBundleWithoutCredentialsPath();
    try (CqlSession session =
        CqlSession.builder()
            .withCloudSecureConnectBundle(bundle)
            .withAuthCredentials("cassandra", "cassandra")
            .build()) {
      set = session.execute("select * from system.local");
    }
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_non_normalized_path() {
    Path bundle = proxyRule.getProxy().getBundlesRootPath().resolve("../bundles/creds-v1.zip");
    ResultSet set;
    try (CqlSession session = CqlSession.builder().withCloudSecureConnectBundle(bundle).build()) {
      set = session.execute("select * from system.local");
    }
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_input_stream() throws IOException {
    InputStream bundle = Files.newInputStream(proxyRule.getProxy().getDefaultBundlePath());
    ResultSet set;
    try (CqlSession session = CqlSession.builder().withCloudSecureConnectBundle(bundle).build()) {
      set = session.execute("select * from system.local");
    }
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_URL() throws IOException {
    // given
    byte[] bundle = Files.readAllBytes(proxyRule.getProxy().getDefaultBundlePath());
    stubFor(
        any(urlEqualTo(BUNDLE_URL_PATH))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(bundle)));
    URL bundleUrl =
        new URL(String.format("http://localhost:%d%s", wireMockRule.port(), BUNDLE_URL_PATH));

    // when
    ResultSet set;
    try (CqlSession session =
        CqlSession.builder().withCloudSecureConnectBundle(bundleUrl).build()) {

      // then
      set = session.execute("select * from system.local");
    }
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_absolute_path_provided_in_the_session_setting() {
    // given
    String bundle = proxyRule.getProxy().getDefaultBundlePath().toString();
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, bundle)
            .build();
    // when
    ResultSet set;
    try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {

      // then
      set = session.execute("select * from system.local");
    }
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_non_normalized_path_provided_in_the_session_setting() {
    // given
    String bundle =
        proxyRule.getProxy().getBundlesRootPath().resolve("../bundles/creds-v1.zip").toString();
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, bundle)
            .build();
    // when
    ResultSet set;
    try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {

      // then
      set = session.execute("select * from system.local");
    }
    assertThat(set).isNotNull();
  }

  @Test
  public void
      should_connect_to_proxy_using_url_with_file_protocol_provided_in_the_session_setting() {
    // given
    String bundle = proxyRule.getProxy().getDefaultBundlePath().toString();
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, bundle)
            .build();
    // when
    ResultSet set;
    try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {

      // then
      set = session.execute("select * from system.local");
    }
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_url_with_http_protocol_provided_in_the_session_setting()
      throws IOException {
    // given
    byte[] bundle = Files.readAllBytes(proxyRule.getProxy().getDefaultBundlePath());
    stubFor(
        any(urlEqualTo(BUNDLE_URL_PATH))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(bundle)));
    String bundleUrl = String.format("http://localhost:%d%s", wireMockRule.port(), BUNDLE_URL_PATH);
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, bundleUrl)
            .build();
    // when
    ResultSet set;
    try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {

      // then
      set = session.execute("select * from system.local");
    }
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_and_log_info_when_contact_points_and_secure_bundle_used() {
    // given
    LoggerTest.LoggerSetup logger = setupTestLogger(SessionBuilder.class, Level.INFO);

    Path bundle = proxyRule.getProxy().getBundleWithoutCredentialsPath();

    try (CqlSession session =
        CqlSession.builder()
            .withCloudSecureConnectBundle(bundle)
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
            .withAuthCredentials("cassandra", "cassandra")
            .build(); ) {

      // when
      ResultSet set = session.execute("select * from system.local");
      // then
      assertThat(set).isNotNull();
      verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains(
              "Both a secure connect bundle and contact points were provided. These are mutually exclusive. The contact points from a secure bundle will have priority.");

    } finally {
      logger.close();
    }
  }

  @Test
  public void should_connect_and_log_info_when_ssl_context_and_secure_bundle_used_programmatic()
      throws NoSuchAlgorithmException {
    // given
    LoggerTest.LoggerSetup logger = setupTestLogger(SessionBuilder.class, Level.INFO);

    Path bundle = proxyRule.getProxy().getBundleWithoutCredentialsPath();

    try (CqlSession session =
        CqlSession.builder()
            .withCloudSecureConnectBundle(bundle)
            .withAuthCredentials("cassandra", "cassandra")
            .withSslContext(SSLContext.getInstance("SSL"))
            .build()) {
      // when
      ResultSet set = session.execute("select * from system.local");
      // then
      assertThat(set).isNotNull();
      verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains(
              "Both withCloudSecureConnectBundle and explicitly specified ssl configuration were provided. They are mutually exclusive. The ssl settings from a secure bundle will have priority.");
    } finally {
      logger.close();
    }
  }

  @Test
  public void should_error_when_ssl_context_and_secure_bundle_used_config()
      throws NoSuchAlgorithmException {
    // given
    LoggerTest.LoggerSetup logger = setupTestLogger(SessionBuilder.class, Level.INFO);

    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.RECONNECT_ON_INIT, true)
            .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
            .build();

    Path bundle = proxyRule.getProxy().getBundleWithoutCredentialsPath();

    try (CqlSession session =
        CqlSession.builder()
            .withConfigLoader(loader)
            .withCloudSecureConnectBundle(bundle)
            .withAuthCredentials("cassandra", "cassandra")
            .build()) {
      // when
      ResultSet set = session.execute("select * from system.local");
      // then
      assertThat(set).isNotNull();
      verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains(
              "Both withCloudSecureConnectBundle and explicitly specified ssl configuration were provided. They are mutually exclusive. The ssl settings from a secure bundle will have priority.");
    } finally {
      logger.close();
    }
  }

  @Test
  public void should_connect_and_log_info_when_local_data_center_and_secure_bundle_used() {
    // given
    LoggerTest.LoggerSetup logger = setupTestLogger(SessionBuilder.class, Level.INFO);

    Path bundle = proxyRule.getProxy().getBundleWithoutCredentialsPath();

    try (CqlSession session =
        CqlSession.builder()
            .withCloudSecureConnectBundle(bundle)
            .withLocalDatacenter("dc-ignored")
            .withAuthCredentials("cassandra", "cassandra")
            .build(); ) {

      // when
      ResultSet set = session.execute("select * from system.local");
      // then
      assertThat(set).isNotNull();
      verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains(
              "Both withCloudSecureConnectBundle and explicitly specified local datacenter configuration were provided. They are mutually exclusive. The local datacenter settings from a secure bundle will have priority.");

    } finally {
      logger.close();
    }
  }
}
