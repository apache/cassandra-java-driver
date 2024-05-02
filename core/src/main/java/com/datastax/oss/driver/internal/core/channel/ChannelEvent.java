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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/** Events relating to driver channels. */
@Immutable
public class ChannelEvent {
  public enum Type {
    OPENED,
    CLOSED,
    RECONNECTION_STARTED,
    RECONNECTION_STOPPED,
    CONTROL_CONNECTION_FAILED
  }

  public static ChannelEvent channelOpened(Node node) {
    return new ChannelEvent(Type.OPENED, node);
  }

  public static ChannelEvent channelClosed(Node node) {
    return new ChannelEvent(Type.CLOSED, node);
  }

  public static ChannelEvent reconnectionStarted(Node node) {
    return new ChannelEvent(Type.RECONNECTION_STARTED, node);
  }

  public static ChannelEvent reconnectionStopped(Node node) {
    return new ChannelEvent(Type.RECONNECTION_STOPPED, node);
  }

  /** The control connection tried to use this node, but failed to open a channel. */
  public static ChannelEvent controlConnectionFailed(Node node) {
    return new ChannelEvent(Type.CONTROL_CONNECTION_FAILED, node);
  }

  public final Type type;
  public final Node node;

  public ChannelEvent(Type type, Node node) {
    this.type = type;
    this.node = node;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ChannelEvent) {
      ChannelEvent that = (ChannelEvent) other;
      return this.type == that.type && Objects.equals(this.node, that.node);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, node);
  }

  @Override
  public String toString() {
    return "ChannelEvent(" + type + ", " + node + ")";
  }
}
