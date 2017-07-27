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
package com.datastax.oss.driver.api.testinfra;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import org.junit.rules.ExternalResource;

/**
 * An {@link ExternalResource} which provides a {@link #setUp()} method for initializing the
 * resource externally (instead of making users use rule chains) and a {@link #getContactPoints()}
 * for accessing the contact points of the cassandra cluster.
 */
public abstract class CassandraResourceRule extends ExternalResource {

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
  public Set<InetSocketAddress> getContactPoints() {
    return Collections.singleton(new InetSocketAddress("127.0.0.1", 9042));
  }

  /** @return The highest protocol version supported by this resource. */
  public abstract ProtocolVersion getHighestProtocolVersion();
}
