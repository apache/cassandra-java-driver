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
package com.datastax.driver.core;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/** An endpoint to access nodes through a proxy that uses SNI routing. */
public class SniEndPoint implements EndPoint {

  private static final AtomicLong OFFSET = new AtomicLong();

  private final InetSocketAddress proxyAddress;
  private final String serverName;

  /**
   * @param proxyAddress the address of the proxy. If it is {@linkplain
   *     InetSocketAddress#isUnresolved() unresolved}, each call to {@link #resolve()} will
   *     re-resolve it, fetch all of its A-records, and if there are more than 1 pick one in a
   *     round-robin fashion.
   * @param serverName the SNI server name. In the context of DSOD, this is the string
   *     representation of the host id.
   */
  public SniEndPoint(InetSocketAddress proxyAddress, String serverName) {
    Preconditions.checkNotNull(proxyAddress);
    Preconditions.checkNotNull(serverName);
    this.proxyAddress = proxyAddress;
    this.serverName = serverName;
  }

  @Override
  public InetSocketAddress resolve() {
    if (proxyAddress.isUnresolved()) {
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
        return new InetSocketAddress(aRecords[index], proxyAddress.getPort());
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException(
            "Could not resolve proxy address " + proxyAddress.getHostName(), e);
      }
    } else {
      return proxyAddress;
    }
  }

  String getServerName() {
    return serverName;
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
    return Objects.hashCode(proxyAddress, serverName);
  }

  @Override
  public String toString() {
    // Note that this uses the original proxy address, so if there are multiple A-records it won't
    // show which one was selected. If that turns out to be a problem for debugging, we might need
    // to store the result of resolve() in Connection and log that instead of the endpoint.
    return proxyAddress.toString() + ":" + serverName;
  }

  private static final Comparator<InetAddress> IP_COMPARATOR =
      new Comparator<InetAddress>() {
        @Override
        public int compare(InetAddress address1, InetAddress address2) {
          return UnsignedBytes.lexicographicalComparator()
              .compare(address1.getAddress(), address2.getAddress());
        }
      };
}
