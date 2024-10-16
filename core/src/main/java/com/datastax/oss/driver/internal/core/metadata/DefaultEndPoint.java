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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;

public class DefaultEndPoint implements EndPoint, Serializable {

  private static final long serialVersionUID = 1;

  private final InetSocketAddress address;
  private final String metricPrefix;

  public DefaultEndPoint(InetSocketAddress address) {
    this.address = Objects.requireNonNull(address, "address can't be null");
    this.metricPrefix = buildMetricPrefix(address);
  }

  @NonNull
  @Override
  public InetSocketAddress resolve() {
    return address;
  }

  @Override
  public InetSocketAddress retrieve() {
    return address;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DefaultEndPoint) {
      InetSocketAddress thisAddress = this.address;
      InetSocketAddress thatAddress = ((DefaultEndPoint) other).address;
      // If only one of the addresses is unresolved, resolve the other. Otherwise (both resolved or
      // both unresolved), compare as-is.
      if (thisAddress.isUnresolved() && !thatAddress.isUnresolved()) {
        thisAddress = new InetSocketAddress(thisAddress.getHostName(), thisAddress.getPort());
      } else if (thatAddress.isUnresolved() && !thisAddress.isUnresolved()) {
        thatAddress = new InetSocketAddress(thatAddress.getHostName(), thatAddress.getPort());
      }
      return thisAddress.equals(thatAddress);
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

  @NonNull
  @Override
  public String asMetricPrefix() {
    return metricPrefix;
  }

  private static String buildMetricPrefix(InetSocketAddress address) {
    String hostString = address.getHostString();
    if (hostString == null) {
      throw new IllegalArgumentException(
          "Could not extract a host string from provided address " + address);
    }
    // Append the port since Cassandra 4 supports nodes with different ports
    return hostString.replace('.', '_') + ':' + address.getPort();
  }
}
