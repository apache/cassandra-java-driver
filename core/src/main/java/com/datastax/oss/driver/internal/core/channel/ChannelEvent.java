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

/** An event to notify other driver components when a channel has been opened or closed. */
public class ChannelEvent {
  public enum Type {
    OPENED,
    CLOSED
  }

  public final Type type;
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
}
