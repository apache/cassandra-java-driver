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
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.shaded.guava.common.primitives.UnsignedBytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SniEndPoint implements EndPoint {
  private static final AtomicLong OFFSET = new AtomicLong();

  private final InetSocketAddress proxyAddress;
  private final String serverName;
  private final InternalDriverContext context;

  /**
   * @param proxyAddress the address of the proxy. If it is {@linkplain
   *     InetSocketAddress#isUnresolved() unresolved}, each call to {@link #resolve()} will
   *     re-resolve it, fetch all of its A-records, and if there are more than 1 pick one in a
   *     round-robin fashion.
   * @param serverName the SNI server name. In the context of Cloud, this is the string
   *     representation of the host id.
   * @param context
   */
  public SniEndPoint(
      InetSocketAddress proxyAddress, String serverName, @Nullable InternalDriverContext context) {
    this.proxyAddress = Objects.requireNonNull(proxyAddress, "SNI address cannot be null");
    this.serverName = Objects.requireNonNull(serverName, "SNI Server name cannot be null");
    this.context = context;
  }

  public String getServerName() {
    return serverName;
  }

  @NonNull
  @Override
  public InetSocketAddress resolve() {
    try {
      InetAddress[] aRecords = InetAddress.getAllByName(proxyAddress.getHostName());
      if (aRecords.length == 0) {
        // Probably never happens, but the JDK docs don't explicitly say so
        throw new IllegalArgumentException(
            "Could not resolve proxy address " + proxyAddress.getHostName());
      }
      // The order of the returned address is unspecified. Sort by IP to make sure we get a true
      // round-robin
      Arrays.sort(aRecords, IP_COMPARATOR);
      int index = (aRecords.length == 1) ? 0 : (int) OFFSET.getAndIncrement() % aRecords.length;
      ArrayUtils.rotate(aRecords, 0, aRecords.length, index);

      // If we have a context, sort the records by active connection count
      if (context != null) {
        Map<InetAddress, Long> activeConnectionCount =
            context.getMetadataManager().getMetadata().getNodes().values().stream()
                .map(context.getPoolManager().getPools()::get)
                .flatMap(
                    pool ->
                        StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(
                                pool.channelIterator(), Spliterator.ORDERED),
                            false))
                .flatMap(
                    channel -> {
                      SocketAddress remote = channel.remoteAddress();
                      if (remote instanceof InetSocketAddress) {
                        return Stream.of((InetSocketAddress) remote);
                      } else {
                        // ignore connections which aren't InetSocketAddress
                        return Stream.empty();
                      }
                    })
                .collect(
                    Collectors.groupingBy(InetSocketAddress::getAddress, Collectors.counting()));
        // sort the records by active count, equal elements won't be re-ordered
        Arrays.sort(
            aRecords,
            Comparator.comparing(aRecord -> activeConnectionCount.getOrDefault(aRecord, 0L)));
      }

      return new InetSocketAddress(aRecords[0], proxyAddress.getPort());
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
