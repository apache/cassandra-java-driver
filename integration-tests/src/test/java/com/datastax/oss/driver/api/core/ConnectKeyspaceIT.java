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
package com.datastax.oss.driver.api.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class ConnectKeyspaceIT {
  @ClassRule public static CcmRule ccm = CcmRule.getInstance();

  @ClassRule public static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @Test
  public void should_connect_to_existing_keyspace() {
    CqlIdentifier keyspace = sessionRule.keyspace();
    try (Session session = SessionUtils.newSession(ccm, keyspace)) {
      assertThat(session.getKeyspace()).hasValue(keyspace);
    }
  }

  @Test
  public void should_connect_with_no_keyspace() {
    try (Session session = SessionUtils.newSession(ccm)) {
      assertThat(session.getKeyspace()).isEmpty();
    }
  }

  @Test(expected = InvalidKeyspaceException.class)
  public void should_fail_to_connect_to_non_existent_keyspace_when_not_reconnecting_on_init() {
    should_fail_to_connect_to_non_existent_keyspace(null);
  }

  @Test(expected = InvalidKeyspaceException.class)
  public void should_fail_to_connect_to_non_existent_keyspace_when_reconnecting_on_init() {
    // Just checking that we don't trigger retries for this unrecoverable error
    should_fail_to_connect_to_non_existent_keyspace(
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.RECONNECT_ON_INIT, true)
            .build());
  }

  private void should_fail_to_connect_to_non_existent_keyspace(DriverConfigLoader loader) {
    CqlIdentifier keyspace = CqlIdentifier.fromInternal("does not exist");
    SessionUtils.newSession(ccm, keyspace, loader);
  }
}
