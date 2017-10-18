/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.auth;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class PlainTextAuthProviderIT {

  @ClassRule
  public static CustomCcmRule ccm =
      CustomCcmRule.builder()
          .withCassandraConfiguration("authenticator", "PasswordAuthenticator")
          .withJvmArgs("-Dcassandra.superuser_setup_delay_ms=0")
          .build();

  @BeforeClass
  public static void sleepForAuth() {
    if (ccm.getCassandraVersion().compareTo(CassandraVersion.V2_2_0) < 0) {
      // Sleep for 1 second to allow C* auth to do its work.  This is only needed for 2.1
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void should_connect_with_credentials() {
    try (Cluster<CqlSession> authCluster =
        ClusterUtils.newCluster(
            ccm,
            "protocol.auth-provider.class = com.datastax.oss.driver.api.core.auth.PlainTextAuthProvider",
            "protocol.auth-provider.username = cassandra",
            "protocol.auth-provider.password = cassandra")) {
      CqlSession session = authCluster.connect();
      session.execute("select * from system.local");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_with_invalid_credentials() {
    try (Cluster<CqlSession> authCluster =
        ClusterUtils.newCluster(
            ccm,
            "protocol.auth-provider.class = com.datastax.oss.driver.api.core.auth.PlainTextAuthProvider",
            "protocol.auth-provider.username = baduser",
            "protocol.auth-provider.password = badpass")) {
      CqlSession session = authCluster.connect();
      session.execute("select * from system.local");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_without_credentials() {
    try (Cluster<CqlSession> plainCluster = ClusterUtils.newCluster(ccm)) {
      CqlSession session = plainCluster.connect();
      session.execute("select * from system.local");
    }
  }
}
