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
package com.datastax.driver.core.cloud;

import static com.datastax.driver.core.cloud.SniProxyServer.CERTS_BUNDLE_SUFFIX;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import org.parboiled.common.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CloudTest {

  private SniProxyServer proxy = new SniProxyServer();

  private WireMockServer wireMockServer;

  @BeforeClass(groups = "short")
  public void startProxy() {
    proxy.startProxy();
  }

  @BeforeMethod(groups = "short")
  public void startWireMock() {
    wireMockServer = new WireMockServer(wireMockConfig().dynamicPort().dynamicHttpsPort());
    wireMockServer.start();
  }

  @AfterMethod(groups = "short")
  public void stopWireMock() {
    wireMockServer.stop();
  }

  @AfterClass(groups = "short", alwaysRun = true)
  public void stopProxy() throws Exception {
    proxy.stopProxy();
  }

  @Test(groups = "short")
  public void should_connect_to_proxy_using_absolute_path() {
    Session session =
        Cluster.builder()
            .withCloudSecureConnectBundle(proxy.getSecureBundleFile())
            .build()
            .connect();
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test(groups = "short")
  public void should_connect_to_proxy_using_non_normalized_path() {
    String path =
        String.format("%s/%s", proxy.getProxyRootPath(), "certs/bundles/../bundles/creds-v1.zip");
    Session session =
        Cluster.builder().withCloudSecureConnectBundle(new File(path)).build().connect();
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test(groups = "short")
  public void should_connect_to_proxy_using_file_provided_by_the_http_URL() throws IOException {
    // given
    wireMockServer.stubFor(
        any(urlEqualTo(CERTS_BUNDLE_SUFFIX))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(FileUtils.readAllBytes(proxy.getSecureBundleFile()))));

    URL configFile =
        new URL(String.format("http://localhost:%d%s", wireMockServer.port(), CERTS_BUNDLE_SUFFIX));

    // when
    Session session = Cluster.builder().withCloudSecureConnectBundle(configFile).build().connect();

    // then
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test(groups = "short")
  public void should_connect_to_proxy_using_file_provided_by_input_stream() throws IOException {
    // given
    wireMockServer.stubFor(
        any(urlEqualTo(CERTS_BUNDLE_SUFFIX))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(FileUtils.readAllBytes(proxy.getSecureBundleFile()))));

    URL configFile =
        new URL(String.format("http://localhost:%d%s", wireMockServer.port(), CERTS_BUNDLE_SUFFIX));

    // when
    Session session =
        Cluster.builder().withCloudSecureConnectBundle(configFile.openStream()).build().connect();

    // then
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test(groups = "short")
  public void should_connect_to_proxy_using_auth_provider() {
    Session session =
        Cluster.builder()
            .withCloudSecureConnectBundle(proxy.getSecureBundleNoCredsPath())
            .withAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
            .build()
            .connect();
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test(groups = "short")
  public void should_not_connect_to_proxy_bad_creds() {
    try {
      Session session =
          Cluster.builder()
              .withCloudSecureConnectBundle(proxy.getSecureBundleNoCredsPath())
              .build()
              .connect();
      fail("Expected an AuthenticationException");
    } catch (AuthenticationException e) {
      assertThat(e).hasMessageStartingWith("Authentication error on host");
    }
  }

  @Test(groups = "short")
  public void should_not_connect_to_proxy() {
    try {
      Session session =
          Cluster.builder()
              .withCloudSecureConnectBundle(proxy.getSecureBundleUnreachable())
              .build()
              .connect();
      fail("Expected an IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageStartingWith("Cannot construct cloud config from the cloudConfigUrl");
    }
  }

  @Test(groups = "short")
  public void should_not_allow_contact_points_and_cloud() {
    try {
      Session session =
          Cluster.builder()
              .addContactPoint("127.0.0.1")
              .withCloudSecureConnectBundle(proxy.getSecureBundleNoCredsPath())
              .withCredentials("cassandra", "cassandra")
              .build()
              .connect();
      fail("Expected an IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e)
          .hasMessageStartingWith(
              "Can't use withCloudSecureConnectBundle if you've already called addContactPoint(s)");
    }
  }

  @Test(groups = "short")
  public void should_not_allow_cloud_with_contact_points_string() {
    try {
      Session session =
          Cluster.builder()
              .withCloudSecureConnectBundle(proxy.getSecureBundleNoCredsPath())
              .addContactPoint("127.0.0.1")
              .withCredentials("cassandra", "cassandra")
              .build()
              .connect();
      fail("Expected an IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e)
          .hasMessageStartingWith(
              "Can't use addContactPoint(s) if you've already called withCloudSecureConnectBundle");
    }
  }

  @Test(groups = "short")
  public void should_not_allow_cloud_with_contact_points_endpoint() {
    try {
      Session session =
          Cluster.builder()
              .withCloudSecureConnectBundle(proxy.getSecureBundleNoCredsPath())
              .addContactPoint(
                  new EndPoint() {
                    @Override
                    public InetSocketAddress resolve() {
                      return null;
                    }
                  })
              .withCredentials("cassandra", "cassandra")
              .build()
              .connect();
      fail("Expected an IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e)
          .hasMessageStartingWith(
              "Can't use addContactPoint(s) if you've already called withCloudSecureConnectBundle");
    }
  }
}
