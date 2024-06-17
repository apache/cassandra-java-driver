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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * Indicates that a {@link Node} was selected in a query plan, but it had no connection available.
 *
 * <p>A common reason to encounter this error is when the configured number of connections per node
 * and requests per connection is not high enough to absorb the overall request rate. This can be
 * mitigated by tuning the following options:
 *
 * <ul>
 *   <li>{@code advanced.connection.pool.local.size};
 *   <li>{@code advanced.connection.pool.remote.size};
 *   <li>{@code advanced.connection.max-requests-per-connection}.
 * </ul>
 *
 * See {@code reference.conf} for more details.
 *
 * <p>Another possibility is when you are trying to direct a request {@linkplain
 * com.datastax.oss.driver.api.core.cql.Statement#setNode(Node) to a particular node}, but that node
 * has no connections available.
 */
public class NodeUnavailableException extends DriverException {

  private final Node node;

  public NodeUnavailableException(Node node) {
    super("No connection was available to " + node, null, null, true);
    this.node = Objects.requireNonNull(node);
  }

  @NonNull
  public Node getNode() {
    return node;
  }

  @Override
  @NonNull
  public DriverException copy() {
    return new NodeUnavailableException(node);
  }
}
