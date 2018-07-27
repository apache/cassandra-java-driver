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
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Covers protocol negotiation for the initial connection to the first contact point. */
@Category(ParallelizableTests.class)
public class ProtocolVersionInitialNegotiationIT {

  @Rule public CcmRule ccm = CcmRule.getInstance();

  @CassandraRequirement(
    min = "2.1",
    max = "2.2",
    description = "required to downgrade to an older version"
  )
  @Test
  public void should_downgrade_to_v3() {
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(3);
      session.execute("select * from system.local");
    }
  }

  @CassandraRequirement(
    min = "2.1",
    max = "2.2",
    description = "required to downgrade to an older version"
  )
  @Test
  public void should_fail_if_provided_version_isnt_supported() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(3);
      session.execute("select * from system.local");
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException anfe) {
      Throwable cause = anfe.getErrors().values().iterator().next();
      assertThat(cause).isInstanceOf(UnsupportedProtocolVersionException.class);
      UnsupportedProtocolVersionException unsupportedException =
          (UnsupportedProtocolVersionException) cause;
      assertThat(unsupportedException.getAttemptedVersions())
          .containsOnly(DefaultProtocolVersion.V4);
    }
  }

  @CassandraRequirement(min = "2.2", description = "required to meet default protocol version")
  @Test
  public void should_not_downgrade_if_server_supports_latest_version() {
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(4);
      session.execute("select * from system.local");
    }
  }

  @CassandraRequirement(min = "2.2", description = "required to use an older protocol version")
  @Test
  public void should_use_explicitly_provided_protocol_version() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V3")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(3);
      session.execute("select * from system.local");
    }
  }
}
