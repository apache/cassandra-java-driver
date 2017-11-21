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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.datastax.oss.driver.Assertions.assertThat;

/**
 * Covers the method that are agnostic to the actual {@link ProtocolVersion} implementation (using a
 * mock implementation).
 */
public class CassandraProtocolVersionRegistryTest {

  private static ProtocolVersion V3 = new MockProtocolVersion(3, false);
  private static ProtocolVersion V4 = new MockProtocolVersion(4, false);
  private static ProtocolVersion V5 = new MockProtocolVersion(5, false);
  private static ProtocolVersion V5_BETA = new MockProtocolVersion(5, true);
  private static ProtocolVersion V10 = new MockProtocolVersion(10, false);
  private static ProtocolVersion V11 = new MockProtocolVersion(11, false);

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void should_fail_if_duplicate_version_code() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Duplicate version code: 5 in V5 and V5_BETA");
    new CassandraProtocolVersionRegistry("test", new ProtocolVersion[] {V5, V5_BETA});
  }

  @Test
  public void should_find_version_by_name() {
    ProtocolVersionRegistry versions =
        new CassandraProtocolVersionRegistry("test", new ProtocolVersion[] {V3, V4});
    assertThat(versions.fromName("V3")).isEqualTo(V3);
    assertThat(versions.fromName("V4")).isEqualTo(V4);
  }

  @Test
  public void should_downgrade_if_lower_version_available() {
    ProtocolVersionRegistry versions =
        new CassandraProtocolVersionRegistry("test", new ProtocolVersion[] {V3, V4});
    Optional<ProtocolVersion> downgraded = versions.downgrade(V4);
    downgraded.map(version -> assertThat(version).isEqualTo(V3)).orElseThrow(AssertionError::new);
  }

  @Test
  public void should_not_downgrade_if_no_lower_version() {
    ProtocolVersionRegistry versions =
        new CassandraProtocolVersionRegistry("test", new ProtocolVersion[] {V3, V4});
    Optional<ProtocolVersion> downgraded = versions.downgrade(V3);
    assertThat(downgraded.isPresent()).isFalse();
  }

  @Test
  public void should_downgrade_across_version_range() {
    ProtocolVersionRegistry versions =
        new CassandraProtocolVersionRegistry(
            "test", new ProtocolVersion[] {V3, V4}, new ProtocolVersion[] {V10, V11});
    Optional<ProtocolVersion> downgraded = versions.downgrade(V10);
    downgraded.map(version -> assertThat(version).isEqualTo(V4)).orElseThrow(AssertionError::new);
  }

  @Test
  public void should_downgrade_skipping_beta_version() {
    ProtocolVersionRegistry versions =
        new CassandraProtocolVersionRegistry(
            "test", new ProtocolVersion[] {V4, V5_BETA}, new ProtocolVersion[] {V10, V11});
    Optional<ProtocolVersion> downgraded = versions.downgrade(V10);
    downgraded.map(version -> assertThat(version).isEqualTo(V4)).orElseThrow(AssertionError::new);
  }

  private static class MockProtocolVersion implements ProtocolVersion {
    private final int code;
    private final boolean beta;

    MockProtocolVersion(int code, boolean beta) {
      this.code = code;
      this.beta = beta;
    }

    @Override
    public int getCode() {
      return code;
    }

    @Override
    public String name() {
      return "V" + code;
    }

    @Override
    public boolean isBeta() {
      return beta;
    }

    @Override
    public String toString() {
      return name() + (beta ? "_BETA" : "");
    }
  }
}
