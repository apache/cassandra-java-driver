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
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.channel.local.LocalAddress;
import java.net.SocketAddress;

/** Endpoint implementation for unit tests that use the local Netty transport. */
public class LocalEndPoint implements EndPoint {

  private final LocalAddress localAddress;

  public LocalEndPoint(String id) {
    this.localAddress = new LocalAddress(id);
  }

  @NonNull
  @Override
  public SocketAddress resolve() {
    return localAddress;
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    throw new UnsupportedOperationException("This should not get called from unit tests");
  }
}
