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
package com.datastax.oss.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.Assume;
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
      description = "Only C* in [2.1,2.2[ has V3 as its highest version")
  @Test
  public void should_downgrade_to_v3_oss() {
    Assume.assumeFalse("This test is only for OSS C*", ccm.getDseVersion().isPresent());
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(3);
      session.execute("select * from system.local");
    }
  }

  @DseRequirement(max = "5.0", description = "Only DSE in [*,5.0[ has V3 as its highest version")
  @Test
  public void should_downgrade_to_v3_dse() {
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(3);
      session.execute("select * from system.local");
    }
  }

  @CassandraRequirement(
      min = "2.2",
      max = "4.0-rc1",
      description = "Only C* in [2.2,4.0-rc1[ has V4 as its highest version")
  @Test
  public void should_downgrade_to_v4_oss() {
    Assume.assumeFalse("This test is only for OSS C*", ccm.getDseVersion().isPresent());
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(4);
      session.execute("select * from system.local");
    }
  }

  @CassandraRequirement(
      min = "4.0-rc1",
      description = "Only C* in [4.0-rc1,*[ has V5 as its highest version")
  @Test
  public void should_downgrade_to_v5_oss() {
    Assume.assumeFalse("This test is only for OSS C*", ccm.getDseVersion().isPresent());
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(5);
      session.execute("select * from system.local");
    }
  }

  @DseRequirement(
      min = "5.0",
      max = "5.1",
      description = "Only DSE in [5.0,5.1[ has V4 as its highest version")
  @Test
  public void should_downgrade_to_v4_dse() {
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(4);
      session.execute("select * from system.local");
    }
  }

  @DseRequirement(
      min = "5.1",
      max = "6.0",
      description = "Only DSE in [5.1,6.0[ has DSE_V1 as its highest version")
  @Test
  public void should_downgrade_to_dse_v1() {
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion()).isEqualTo(DseProtocolVersion.DSE_V1);
      session.execute("select * from system.local");
    }
  }

  @CassandraRequirement(max = "2.2", description = "Only C* in [*,2.2[ has V4 unsupported")
  @Test
  public void should_fail_if_provided_v4_is_not_supported_oss() {
    Assume.assumeFalse("This test is only for OSS C*", ccm.getDseVersion().isPresent());
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
            .build();
    try (CqlSession ignored = SessionUtils.newSession(ccm, loader)) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException anfe) {
      Throwable cause = anfe.getAllErrors().values().iterator().next().get(0);
      assertThat(cause).isInstanceOf(UnsupportedProtocolVersionException.class);
      UnsupportedProtocolVersionException unsupportedException =
          (UnsupportedProtocolVersionException) cause;
      assertThat(unsupportedException.getAttemptedVersions())
          .containsOnly(DefaultProtocolVersion.V4);
    }
  }

  @DseRequirement(max = "5.0", description = "Only DSE in [*,5.0[ has V4 unsupported")
  @Test
  public void should_fail_if_provided_v4_is_not_supported_dse() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
            .build();
    try (CqlSession ignored = SessionUtils.newSession(ccm, loader)) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException anfe) {
      Throwable cause = anfe.getAllErrors().values().iterator().next().get(0);
      assertThat(cause).isInstanceOf(UnsupportedProtocolVersionException.class);
      UnsupportedProtocolVersionException unsupportedException =
          (UnsupportedProtocolVersionException) cause;
      assertThat(unsupportedException.getAttemptedVersions())
          .containsOnly(DefaultProtocolVersion.V4);
    }
  }

  @CassandraRequirement(
      min = "2.1",
      max = "4.0-rc1",
      description = "Only C* in [2.1,4.0-rc1[ has V5 unsupported or supported as beta")
  @Test
  public void should_fail_if_provided_v5_is_not_supported_oss() {
    Assume.assumeFalse("This test is only for OSS C*", ccm.getDseVersion().isPresent());
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V5")
            .build();
    try (CqlSession ignored = SessionUtils.newSession(ccm, loader)) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException anfe) {
      Throwable cause = anfe.getAllErrors().values().iterator().next().get(0);
      assertThat(cause).isInstanceOf(UnsupportedProtocolVersionException.class);
      UnsupportedProtocolVersionException unsupportedException =
          (UnsupportedProtocolVersionException) cause;
      assertThat(unsupportedException.getAttemptedVersions())
          .containsOnly(DefaultProtocolVersion.V5);
    }
  }

  @DseRequirement(
      max = "7.0",
      description = "Only DSE in [*,7.0[ has V5 unsupported or supported as beta")
  @Test
  public void should_fail_if_provided_v5_is_not_supported_dse() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V5")
            .build();
    try (CqlSession ignored = SessionUtils.newSession(ccm, loader)) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException anfe) {
      Throwable cause = anfe.getAllErrors().values().iterator().next().get(0);
      assertThat(cause).isInstanceOf(UnsupportedProtocolVersionException.class);
      UnsupportedProtocolVersionException unsupportedException =
          (UnsupportedProtocolVersionException) cause;
      assertThat(unsupportedException.getAttemptedVersions())
          .containsOnly(DefaultProtocolVersion.V5);
    }
  }

  @DseRequirement(max = "5.1", description = "Only DSE in [*,5.1[ has DSE_V1 unsupported")
  @Test
  public void should_fail_if_provided_dse_v1_is_not_supported() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "DSE_V1")
            .build();
    try (CqlSession ignored = SessionUtils.newSession(ccm, loader)) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException anfe) {
      Throwable cause = anfe.getAllErrors().values().iterator().next().get(0);
      assertThat(cause).isInstanceOf(UnsupportedProtocolVersionException.class);
      UnsupportedProtocolVersionException unsupportedException =
          (UnsupportedProtocolVersionException) cause;
      assertThat(unsupportedException.getAttemptedVersions())
          .containsOnly(DseProtocolVersion.DSE_V1);
    }
  }

  @DseRequirement(max = "6.0", description = "Only DSE in [*,6.0[ has DSE_V2 unsupported")
  @Test
  public void should_fail_if_provided_dse_v2_is_not_supported() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "DSE_V2")
            .build();
    try (CqlSession ignored = SessionUtils.newSession(ccm, loader)) {
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException anfe) {
      Throwable cause = anfe.getAllErrors().values().iterator().next().get(0);
      assertThat(cause).isInstanceOf(UnsupportedProtocolVersionException.class);
      UnsupportedProtocolVersionException unsupportedException =
          (UnsupportedProtocolVersionException) cause;
      assertThat(unsupportedException.getAttemptedVersions())
          .containsOnly(DseProtocolVersion.DSE_V2);
    }
  }

  /** Note that this test will need to be updated as new protocol versions are introduced. */
  @CassandraRequirement(min = "4.0", description = "Only C* in [4.0,*[ has V5 supported")
  @Test
  public void should_not_downgrade_if_server_supports_latest_version_oss() {
    Assume.assumeFalse("This test is only for OSS C*", ccm.getDseVersion().isPresent());
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion()).isEqualTo(ProtocolVersion.V5);
      session.execute("select * from system.local");
    }
  }

  /** Note that this test will need to be updated as new protocol versions are introduced. */
  @DseRequirement(min = "6.0", description = "Only DSE in [6.0,*[ has DSE_V2 supported")
  @Test
  public void should_not_downgrade_if_server_supports_latest_version_dse() {
    try (CqlSession session = SessionUtils.newSession(ccm)) {
      assertThat(session.getContext().getProtocolVersion()).isEqualTo(ProtocolVersion.DSE_V2);
      session.execute("select * from system.local");
    }
  }

  @CassandraRequirement(min = "2.1", description = "Only C* in [2.1,*[ has V3 supported")
  @Test
  public void should_use_explicitly_provided_v3_oss() {
    Assume.assumeFalse("This test is only for OSS C*", ccm.getDseVersion().isPresent());
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V3")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(3);
      session.execute("select * from system.local");
    }
  }

  @DseRequirement(min = "4.8", description = "Only DSE in [4.8,*[ has V3 supported")
  @Test
  public void should_use_explicitly_provided_v3_dse() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V3")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(3);
      session.execute("select * from system.local");
    }
  }

  @CassandraRequirement(min = "2.2", description = "Only C* in [2.2,*[ has V4 supported")
  @Test
  public void should_use_explicitly_provided_v4_oss() {
    Assume.assumeFalse("This test is only for OSS C*", ccm.getDseVersion().isPresent());
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(4);
      session.execute("select * from system.local");
    }
  }

  @DseRequirement(min = "5.0", description = "Only DSE in [5.0,*[ has V4 supported")
  @Test
  public void should_use_explicitly_provided_v4_dse() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(4);
      session.execute("select * from system.local");
    }
  }

  @CassandraRequirement(min = "4.0", description = "Only C* in [4.0,*[ has V5 supported")
  @Test
  public void should_use_explicitly_provided_v5_oss() {
    Assume.assumeFalse("This test is only for OSS C*", ccm.getDseVersion().isPresent());
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V5")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(5);
      session.execute("select * from system.local");
    }
  }

  @DseRequirement(min = "7.0", description = "Only DSE in [7.0,*[ has V5 supported")
  @Test
  public void should_use_explicitly_provided_v5_dse() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V5")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion().getCode()).isEqualTo(5);
      session.execute("select * from system.local");
    }
  }

  @DseRequirement(min = "5.1", description = "Only DSE in [5.1,*[ has DSE_V1 supported")
  @Test
  public void should_use_explicitly_provided_dse_v1() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "DSE_V1")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion()).isEqualTo(DseProtocolVersion.DSE_V1);
      session.execute("select * from system.local");
    }
  }

  @DseRequirement(min = "6.0", description = "Only DSE in [6.0,*[ has DSE_V2 supported")
  @Test
  public void should_use_explicitly_provided_dse_v2() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "DSE_V2")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      assertThat(session.getContext().getProtocolVersion()).isEqualTo(DseProtocolVersion.DSE_V2);
      session.execute("select * from system.local");
    }
  }
}
