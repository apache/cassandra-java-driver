/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.testinfra.session;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;

public abstract class SessionRuleBuilder<
    SelfT extends SessionRuleBuilder<SelfT, SessionT>, SessionT extends Session> {

  protected final CassandraResourceRule cassandraResource;
  protected boolean createKeyspace = true;
  protected NodeStateListener nodeStateListener;
  protected SchemaChangeListener schemaChangeListener;
  protected DriverConfigLoader loader;

  @SuppressWarnings("unchecked")
  protected final SelfT self = (SelfT) this;

  public SessionRuleBuilder(CassandraResourceRule cassandraResource) {
    this.cassandraResource = cassandraResource;
  }

  /**
   * Whether to create a keyspace.
   *
   * <p>If this is set, the rule will create a keyspace with a name unique to this test (this allows
   * multiple tests to run concurrently against the same server resource), and make the name
   * available through {@link SessionRule#keyspace()}. The created session will be connected to this
   * keyspace.
   *
   * <p>If this method is not called, the default value is {@code true}.
   *
   * <p>Note that this option is only valid with a {@link CcmRule}. If the server resource is a
   * {@link SimulacronRule}, this option is ignored, no keyspace gets created, and {@link
   * SessionRule#keyspace()} returns {@code null}.
   */
  public SelfT withKeyspace(boolean createKeyspace) {
    this.createKeyspace = createKeyspace;
    return self;
  }

  /** A set of options to override in the session configuration. */
  public SelfT withConfigLoader(DriverConfigLoader loader) {
    this.loader = loader;
    return self;
  }

  public SelfT withNodeStateListener(NodeStateListener listener) {
    this.nodeStateListener = listener;
    return self;
  }

  public SelfT withSchemaChangeListener(SchemaChangeListener listener) {
    this.schemaChangeListener = listener;
    return self;
  }

  public abstract SessionRule<SessionT> build();
}
