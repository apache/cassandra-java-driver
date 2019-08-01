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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.categories.IsolatedTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IsolatedTests.class)
public class DbaasIT {

  @ClassRule public static SniProxyRule proxyRule = new SniProxyRule();

  @Test
  public void should_connect_to_proxy() {
    CqlSession session =
        CqlSession.builder()
            .withCloudSecureConnectBundle(proxyRule.getProxy().getSecureBundlePath())
            .build();
    ResultSet set = session.execute("select * from system.local");
    assertThat(set).isNotNull();
  }

  @Test
  public void should_not_connect_to_proxy() {
    try (CqlSession session =
        CqlSession.builder()
            .withCloudSecureConnectBundle(proxyRule.getProxy().getSecureBundleUnreachable())
            .build()) {
      fail("Expected an IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageStartingWith("Unable to construct cloud configuration");
    }
  }
}
