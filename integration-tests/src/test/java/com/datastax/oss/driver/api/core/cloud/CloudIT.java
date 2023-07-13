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
package com.datastax.oss.driver.api.core.cloud;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
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
  public void should_error_when_contact_points_and_secure_bundle_used() {
    // given
    Path bundle = proxyRule.getProxy().getBundleWithoutCredentialsPath();
    CqlSessionBuilder builder =
        CqlSession.builder()
            .withCloudSecureConnectBundle(bundle)
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
            .withAuthCredentials("cassandra", "cassandra");
    assertThatThrownBy(() -> builder.build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Can't use withCloudSecureConnectBundle and addContactPoint(s). They are mutually exclusive.");
  }
}
