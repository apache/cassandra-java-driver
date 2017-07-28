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
package com.datastax.oss.driver.api.testinfra.cluster;

import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;

public class ClusterRuleBuilder {

  private final CassandraResourceRule cassandraResource;
  private boolean createDefaultSession = true;
  private boolean createKeyspace = true;
  private String[] options = new String[] {};

  public ClusterRuleBuilder(CassandraResourceRule cassandraResource) {
    this.cassandraResource = cassandraResource;
  }

  /**
   * Whether to create a keyspace.
   *
   * <p>If this is set, the rule will create a keyspace with a name unique to this test (this allows
   * multiple tests to run concurrently against the same server resource), and make the name
   * available through {@link ClusterRule#keyspace()}. If a {@link #createDefaultSession default
   * session} is created, it will be connected to this keyspace.
   *
   * <p>If this method is not called, the default value is {@code true}.
   *
   * <p>Note that this option is only valid with a {@link CcmRule}. If the server resource is a
   * {@link SimulacronRule}, this option is ignored, no keyspace gets created, and {@link
   * ClusterRule#keyspace()} returns {@code null}.
   */
  public ClusterRuleBuilder withKeyspace(boolean createKeyspace) {
    this.createKeyspace = createKeyspace;
    return this;
  }

  /**
   * Whether to create a default session from the {@code Cluster}.
   *
   * <p>If this is set, the rule will create a session and make it available through {@link
   * ClusterRule#session()}. If a {@link #createKeyspace keyspace} was created, the session will be
   * connected to it.
   *
   * <p>If this method is not called, the default value is {@code true}.
   */
  public ClusterRuleBuilder withDefaultSession(boolean createDefaultSession) {
    this.createDefaultSession = createDefaultSession;
    return this;
  }

  /** A set of options to override in the cluster configuration. */
  public ClusterRuleBuilder withOptions(String... options) {
    this.options = options;
    return this;
  }

  public ClusterRule build() {
    return new ClusterRule(cassandraResource, createKeyspace, createDefaultSession, options);
  }
}
