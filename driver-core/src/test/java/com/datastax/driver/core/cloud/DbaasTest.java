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

import static java.lang.System.clearProperty;
import static java.lang.System.setProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DbaasTest {

  SniProxyServer proxy = new SniProxyServer();

  @BeforeClass(groups = "short")
  public void startProxy() {
    proxy.startProxy();
  }

  @AfterClass(groups = "short", alwaysRun = true)
  public void stopProxy() throws Exception {
    proxy.stopProxy();
  }

  @Test(groups = "short")
  public void should_connect_to_proxy() {
    Session session =
        Cluster.builder()
            .withCloudSecureConnectBundle(proxy.getSecureBundlePath())
            .build()
            .connect();
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test(groups = "short")
  public void should_connect_to_proxy_typesafe_creds() {
    try {
      setProperty("advanced.auth-provider.username", "cassandra");
      setProperty("advanced.auth-provider.password", "cassandra");
      Session session =
          Cluster.builder()
              .withCloudSecureConnectBundle(proxy.getSecureBundlePath())
              .build()
              .connect();
      ResultSet set = session.execute("select * from system.local");
      assertThat(set).isNotNull();
    } finally {
      clearProperty("advanced.auth-provider.username");
      clearProperty("advanced.auth-provider.password");
    }
  }
  // TODO Add this test when sni_endpoint is fixed
  public void should_not_connect_to_proxy_bad_creds() {
    try {
      Session session =
          Cluster.builder()
              .withCloudSecureConnectBundle(proxy.getSecureBundleNoCredsPath())
              .build()
              .connect();
      fail("Expected an IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageStartingWith("Unable to construct cloud configuration");
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
      assertThat(e).hasMessageStartingWith("Unable to construct cloud configuration");
    }
  }
}
