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
package com.datastax.oss.driver.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import org.assertj.core.api.AbstractAssert;

public class NodeMetadataAssert extends AbstractAssert<NodeMetadataAssert, Node> {

  public NodeMetadataAssert(Node actual) {
    super(actual, NodeMetadataAssert.class);
  }

  public NodeMetadataAssert isUp() {
    assertThat(actual.getState()).isSameAs(NodeState.UP);
    return this;
  }

  public NodeMetadataAssert isDown() {
    assertThat(actual.getState()).isSameAs(NodeState.DOWN);
    return this;
  }

  public NodeMetadataAssert isUnknown() {
    assertThat(actual.getState()).isSameAs(NodeState.UNKNOWN);
    return this;
  }

  public NodeMetadataAssert isForcedDown() {
    assertThat(actual.getState()).isSameAs(NodeState.FORCED_DOWN);
    return this;
  }

  public NodeMetadataAssert hasOpenConnections(int expected) {
    assertThat(actual.getOpenConnections()).isEqualTo(expected);
    return this;
  }

  public NodeMetadataAssert isReconnecting() {
    assertThat(actual.isReconnecting()).isTrue();
    return this;
  }

  public NodeMetadataAssert isNotReconnecting() {
    assertThat(actual.isReconnecting()).isFalse();
    return this;
  }

  public NodeMetadataAssert isLocal() {
    assertThat(actual.getDistance()).isSameAs(NodeDistance.LOCAL);
    return this;
  }

  public NodeMetadataAssert isRemote() {
    assertThat(actual.getDistance()).isSameAs(NodeDistance.REMOTE);
    return this;
  }

  public NodeMetadataAssert isIgnored() {
    assertThat(actual.getDistance()).isSameAs(NodeDistance.IGNORED);
    return this;
  }
}
