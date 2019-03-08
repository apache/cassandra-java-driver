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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.SocketAddress;

/** Endpoint implementation for unit tests that use an embedded Netty channel. */
public class EmbeddedEndPoint implements EndPoint {

  private final SocketAddress address;

  public EmbeddedEndPoint(EmbeddedChannel channel) {
    this.address = channel.remoteAddress();
  }

  @Override
  public SocketAddress resolve() {
    throw new UnsupportedOperationException("This should not get called from unit tests");
  }

  @Override
  public String asMetricPrefix() {
    throw new UnsupportedOperationException("This should not get called from unit tests");
  }
}
