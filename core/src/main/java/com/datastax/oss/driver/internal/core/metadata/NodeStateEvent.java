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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * The transition of a node from one {@linkplain NodeState state} to another.
 *
 * <p>For simplicity, this is also used to represent a node addition ({@code oldState=null,
 * newState=UNKNOWN}) or removal ({@code oldState=newState=null}).
 */
@Immutable
public class NodeStateEvent {
  public static NodeStateEvent changed(NodeState oldState, NodeState newState, DefaultNode node) {
    Preconditions.checkNotNull(oldState);
    Preconditions.checkNotNull(newState);
    return new NodeStateEvent(oldState, newState, node);
  }

  public static NodeStateEvent added(DefaultNode node) {
    return new NodeStateEvent(null, NodeState.UNKNOWN, node);
  }

  public static NodeStateEvent removed(DefaultNode node) {
    return new NodeStateEvent(null, null, node);
  }

  /** The state before the change, or {@code null} if this is an addition or a removal. */
  public final NodeState oldState;

  /**
   * The state after the change ({@link NodeState#UNKNOWN} if the node was just added), or {@code
   * null} if this is a removal.
   */
  public final NodeState newState;

  public final DefaultNode node;

  private NodeStateEvent(NodeState oldState, NodeState newState, DefaultNode node) {
    this.node = node;
    this.oldState = oldState;
    this.newState = newState;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof NodeStateEvent) {
      NodeStateEvent that = (NodeStateEvent) other;
      return this.oldState == that.oldState
          && this.newState == that.newState
          && Objects.equals(this.node, that.node);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(oldState, newState, node);
  }

  @Override
  public String toString() {
    return "NodeStateEvent(" + oldState + "=>" + newState + ", " + node + ")";
  }
}
