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
import com.datastax.oss.driver.internal.core.resolver.Resolver;
import com.datastax.oss.driver.internal.core.resolver.ResolverProvider;
import com.datastax.oss.driver.shaded.guava.common.primitives.UnsignedBytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class SniEndPoint implements EndPoint {
  private static final AtomicLong OFFSET = new AtomicLong();
  private static Resolver RESOLVER = ResolverProvider.getResolver(SniEndPoint.class);

  private final InetSocketAddress proxyAddress;
  private final String serverName;

  /**
   * @param proxyAddress the address of the proxy. If it is {@linkplain
   *     InetSocketAddress#isUnresolved() unresolved}, each call to {@link #resolve()} will
   *     re-resolve it, fetch all of its A-records, and if there are more than 1 pick one in a
   *     round-robin fashion.
   * @param serverName the SNI server name. In the context of Cloud, this is the string
   *     representation of the host id.
   */
  public SniEndPoint(InetSocketAddress proxyAddress, String serverName) {
    this.proxyAddress = Objects.requireNonNull(proxyAddress, "SNI address cannot be null");
    this.serverName = Objects.requireNonNull(serverName, "SNI Server name cannot be null");
  }

  public String getServerName() {
    return serverName;
  }

  @NonNull
  @Override
  public InetSocketAddress resolve() {
    try {
      InetAddress[] aRecords = RESOLVER.getAllByName(proxyAddress.getHostName());
      if (aRecords.length == 0) {
        // Probably never happens, but the JDK docs don't explicitly say so
        throw new IllegalArgumentException(
            "Could not resolve proxy address " + proxyAddress.getHostName());
      }
      // The order of the returned address is unspecified. Sort by IP to make sure we get a true
      // round-robin
      Arrays.sort(aRecords, IP_COMPARATOR);
      int index = (aRecords.length == 1) ? 0 : (int) OFFSET.getAndIncrement() % aRecords.length;
      return new InetSocketAddress(aRecords[index], proxyAddress.getPort());
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(
          "Could not resolve proxy address " + proxyAddress.getHostName(), e);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof SniEndPoint) {
      SniEndPoint that = (SniEndPoint) other;
      return this.proxyAddress.equals(that.proxyAddress) && this.serverName.equals(that.serverName);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(proxyAddress, serverName);
  }

  @Override
  public String toString() {
    // Note that this uses the original proxy address, so if there are multiple A-records it won't
    // show which one was selected. If that turns out to be a problem for debugging, we might need
    // to store the result of resolve() in Connection and log that instead of the endpoint.
    return proxyAddress.toString() + ":" + serverName;
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    String hostString = proxyAddress.getHostString();
    if (hostString == null) {
      throw new IllegalArgumentException(
          "Could not extract a host string from provided proxy address " + proxyAddress);
    }
    return hostString.replace('.', '_') + ':' + proxyAddress.getPort() + '_' + serverName;
  }

  @SuppressWarnings("UnnecessaryLambda")
  private static final Comparator<InetAddress> IP_COMPARATOR =
      (InetAddress address1, InetAddress address2) ->
          UnsignedBytes.lexicographicalComparator()
              .compare(address1.getAddress(), address2.getAddress());
}
