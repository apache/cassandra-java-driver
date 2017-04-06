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
package com.datastax.oss.driver.internal.core.channel;

import java.net.SocketAddress;
import java.util.Objects;

/** Events relating to driver channels. */
public class ChannelEvent {
  public enum Type {
    OPENED,
    CLOSED,
    RECONNECTION_STARTED,
    RECONNECTION_STOPPED
  }

  public static ChannelEvent channelOpened(SocketAddress address) {
    return new ChannelEvent(Type.OPENED, address);
  }

  public static ChannelEvent channelClosed(SocketAddress address) {
    return new ChannelEvent(Type.CLOSED, address);
  }

  public static ChannelEvent reconnectionStarted(SocketAddress address) {
    return new ChannelEvent(Type.RECONNECTION_STARTED, address);
  }

  public static ChannelEvent reconnectionStopped(SocketAddress address) {
    return new ChannelEvent(Type.RECONNECTION_STOPPED, address);
  }

  public final Type type;
  /**
   * We use SocketAddress because some of our tests use the local Netty transport, but in production
   * it will always be InetSocketAddress.
   */
  public final SocketAddress address;

  public ChannelEvent(Type type, SocketAddress address) {
    this.type = type;
    this.address = address;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ChannelEvent) {
      ChannelEvent that = (ChannelEvent) other;
      return this.type == that.type && Objects.equals(this.address, that.address);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, address);
  }

  @Override
  public String toString() {
    return "ChannelEvent(" + type + ", " + address + ")";
  }
}
