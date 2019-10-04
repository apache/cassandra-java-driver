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

import static com.datastax.oss.driver.api.core.cloud.SniProxyServer.CERTS_BUNDLE_SUFFIX;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IsolatedTests.class)
public class DbaasIT {

  @Rule
  public WireMockRule wireMockRule =
      new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());

  @ClassRule public static SniProxyRule proxyRule = new SniProxyRule();

  @Test
  public void should_connect_to_proxy_using_absolute_path() {
    CqlSession session =
        CqlSession.builder()
            .withCloudSecureConnectBundle(Paths.get(proxyRule.getProxy().getSecureBundlePath()))
            .build();
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_relative_path() {
    CqlSession session =
        CqlSession.builder()
            .withCloudSecureConnectBundle(
                Paths.get(proxyRule.getProxy().getSecureBundleRelativePath()))
            .build();
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_file_provided_by_the_http_URL() throws IOException {
    // given
    stubFor(
        any(urlEqualTo(CERTS_BUNDLE_SUFFIX))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(
                        Files.readAllBytes(
                            Paths.get(proxyRule.getProxy().getSecureBundlePath())))));

    URL configFile =
        new URL(String.format("http://localhost:%d%s", wireMockRule.port(), CERTS_BUNDLE_SUFFIX));

    // when
    CqlSession session = CqlSession.builder().withCloudSecureConnectBundle(configFile).build();

    // then
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_absolute_path_provided_in_the_session_setting() {
    // given
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(
                DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE,
                proxyRule.getProxy().getSecureBundlePath())
            .build();
    // when
    CqlSession session = CqlSession.builder().withConfigLoader(loader).build();

    // then
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_relative_path_provided_in_the_session_setting() {
    // given
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(
                DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE,
                proxyRule.getProxy().getSecureBundleRelativePath())
            .build();
    // when
    CqlSession session = CqlSession.builder().withConfigLoader(loader).build();

    // then
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_url_with_file_protocol_provided_in_the_session_setting()
      throws MalformedURLException {
    // given
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(
                DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE,
                Paths.get(proxyRule.getProxy().getSecureBundlePath()).toUri().toURL().toString())
            .build();
    // when
    CqlSession session = CqlSession.builder().withConfigLoader(loader).build();

    // then
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test
  public void should_connect_to_proxy_using_url_with_http_protocol_provided_in_the_session_setting()
      throws IOException {
    // given
    stubFor(
        any(urlEqualTo(CERTS_BUNDLE_SUFFIX))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(
                        Files.readAllBytes(
                            Paths.get(proxyRule.getProxy().getSecureBundlePath())))));
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(
                DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE,
                String.format("http://localhost:%d%s", wireMockRule.port(), CERTS_BUNDLE_SUFFIX))
            .build();
    // when
    CqlSession session = CqlSession.builder().withConfigLoader(loader).build();

    // then
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test
  public void should_not_connect_to_proxy() {
    try (CqlSession session =
        CqlSession.builder()
            .withCloudSecureConnectBundle(
                Paths.get(proxyRule.getProxy().getSecureBundleUnreachable()))
            .build()) {
      fail("Expected an IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageStartingWith("Unable to construct cloud configuration");
    }
  }
}
