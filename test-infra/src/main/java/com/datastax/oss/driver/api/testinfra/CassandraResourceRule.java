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
package com.datastax.oss.driver.api.testinfra;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;

/**
 * An {@link ExternalResource} which provides a {@link #getContactPoints()} for accessing the
 * contact points of the cassandra cluster.
 */
public abstract class CassandraResourceRule extends ExternalResource {

  /**
   * @deprecated this method is preserved for backward compatibility only. The correct way to ensure
   *     that a {@code CassandraResourceRule} gets initialized before a {@link SessionRule} is to
   *     wrap them into a {@link RuleChain}. Therefore there is no need to force the initialization
   *     of a {@code CassandraResourceRule} explicitly anymore.
   */
  @Deprecated
  public synchronized void setUp() {
    try {
      this.before();
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /**
   * @return Default contact points associated with this cassandra resource. By default returns
   *     127.0.0.1
   */
  public Set<EndPoint> getContactPoints() {
    return Collections.singleton(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042)));
  }

  /** @return The highest protocol version supported by this resource. */
  public abstract ProtocolVersion getHighestProtocolVersion();
}
