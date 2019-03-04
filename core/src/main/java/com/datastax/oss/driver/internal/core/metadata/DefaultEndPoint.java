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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class DefaultEndPoint implements EndPoint {

  private final InetSocketAddress address;
  private final String metricPrefix;

  public DefaultEndPoint(InetSocketAddress address) {
    Preconditions.checkNotNull(address);
    this.address = address;
    this.metricPrefix = buildMetricPrefix(address);
  }

  @Override
  public InetSocketAddress resolve() {
    return address;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DefaultEndPoint) {
      DefaultEndPoint that = (DefaultEndPoint) other;
      return this.address.equals(that.address);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return address.hashCode();
  }

  @Override
  public String toString() {
    return address.toString();
  }

  @Override
  public String asMetricPrefix() {
    return metricPrefix;
  }

  private static String buildMetricPrefix(InetSocketAddress addressAndPort) {
    StringBuilder prefix = new StringBuilder();
    InetAddress address = addressAndPort.getAddress();
    int port = addressAndPort.getPort();
    if (address instanceof Inet4Address) {
      // Metrics use '.' as a delimiter, replace so that the IP is a single path component
      // (127.0.0.1 => 127_0_0_1)
      prefix.append(address.getHostAddress().replace('.', '_'));
    } else {
      assert address instanceof Inet6Address;
      // IPv6 only uses '%' and ':' as separators, so no replacement needed
      prefix.append(address.getHostAddress());
    }
    // Append the port since Cassandra 4 supports nodes with different ports
    return prefix.append(':').append(port).toString();
  }
}
