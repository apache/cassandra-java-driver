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

import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;

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
    try (Cluster<CqlSession> v3cluster = ClusterUtils.newCluster(ccm)) {
      assertThat(v3cluster.getContext().protocolVersion().getCode()).isEqualTo(3);

      CqlSession session = v3cluster.connect();
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
    try (Cluster<CqlSession> v4cluster = ClusterUtils.newCluster(ccm, "protocol.version = V4")) {
      assertThat(v4cluster.getContext().protocolVersion().getCode()).isEqualTo(3);

      CqlSession session = v4cluster.connect();
      session.execute("select * from system.local");
    } catch (AllNodesFailedException anfe) {
      Throwable cause = anfe.getErrors().values().iterator().next();
      assertThat(cause).isInstanceOf(UnsupportedProtocolVersionException.class);
      UnsupportedProtocolVersionException unsupportedException =
          (UnsupportedProtocolVersionException) cause;
      assertThat(unsupportedException.getAttemptedVersions()).containsOnly(CoreProtocolVersion.V4);
    }
  }

  @CassandraRequirement(min = "2.2", description = "required to meet default protocol version")
  @Test
  public void should_not_downgrade_if_server_supports_latest_version() {
    try (Cluster<CqlSession> v4cluster = ClusterUtils.newCluster(ccm)) {
      assertThat(v4cluster.getContext().protocolVersion().getCode()).isEqualTo(4);

      CqlSession session = v4cluster.connect();
      session.execute("select * from system.local");
    }
  }

  @CassandraRequirement(min = "2.2", description = "required to use an older protocol version")
  @Test
  public void should_use_explicitly_provided_protocol_version() {
    try (Cluster<CqlSession> v3cluster = ClusterUtils.newCluster(ccm, "protocol.version = V3")) {
      assertThat(v3cluster.getContext().protocolVersion().getCode()).isEqualTo(3);

      CqlSession session = v3cluster.connect();
      session.execute("select * from system.local");
    }
  }
}
